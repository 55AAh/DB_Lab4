import pymongo
from fs import Fs
from db import Db, DbOperation, db_session
from datafiles import get_file_encoding, get_file_size, format_file_size, strip, strip_arr, read_file
from user import get_env, print_flush, is_panic


class Populate:
    def __init__(self):
        auth = dict(url=get_env("MONGO_URL"),
                    db_name=get_env("MONGO_DBNAME"))
        self.target_collection_name = get_env("TARGET_COLLECTION_NAME")
        self.target_collection = None
        self.aux_collection_name = get_env("AUX_COLLECTION_NAME")
        self.aux_collection = None

        self.fs = Fs()
        self.db = Db(auth)

    def __enter__(self):
        self.fs.connect()
        self.db.connect()

    def __exit__(self, _exc_type, _exc_val, _exc_tb):
        if is_panic():
            return
        self.db.disconnect()
        self.fs.disconnect()

    def get_session(self):
        return self.db.client.start_session()

    def get_database(self, session):
        return session.client[self.db.auth["db_name"]]

    def get_target_collection(self, session, db):
        return DbOperation(session, db).get_collection(self.target_collection_name, "GET TARGET COLLECTION")

    def get_aux_collection(self, session, db):
        return DbOperation(session, db).get_collection(self.aux_collection_name, "GET AUX COLLECTION")

    def get_state(self):
        @db_session
        def _get_state(session, db):
            target_collection_exists = DbOperation(session, db).check_collection_exists(
                self.target_collection_name, "GET TARGET COLLECTION")
            aux_collection_exists = DbOperation(session, db).check_collection_exists(
                self.aux_collection_name, "GET AUX COLLECTION")

            if aux_collection_exists:
                return "interrupted"
            else:
                if target_collection_exists:
                    return "finished"
                else:
                    return "clear"

        return _get_state(self)

    def drop_target(self):
        @db_session
        def _drop_target(session, db):
            DbOperation(session, db).drop_collection(self.get_target_collection(session, db), "DROP TARGET COLLECTION")

        return _drop_target(self)

    def drop_aux(self):
        @db_session
        def _drop_aux(session, db):
            DbOperation(session, db).drop_collection(self.get_aux_collection(session, db), "DROP AUX COLLECTION")

        return _drop_aux(self)

    def start(self):
        @db_session
        def _get_entries(session, db):
            aux_collection = self.get_aux_collection(session, db)
            return aux_collection.find().sort("tr_id", pymongo.DESCENDING)

        entries = _get_entries(self)
        if entries.count() == 0:
            self.drop_aux()
            return True

        entry = entries[0]
        entry_id, file_name, year, file_seek, header_text, tr_id = \
            entry["_id"], entry["file_name"], entry["year"], entry["file_seek"], entry["header"], entry["tr_id"]
        file_size = get_file_size(file_name)
        print_flush(f"Populating from file '{file_name}' ({year}): ", end='')
        with open(file_name, "r", encoding=get_file_encoding(file_name)) as file:
            if file_seek == 0:
                entry["header"] = header_text = file.readline().strip()
                entry["file_seek"] = file.tell()

                @db_session
                def _set_header_text(session, db):
                    aux_collection = self.get_aux_collection(session, db)
                    DbOperation(session, db).update_data(aux_collection, entry, "SAVE AUX HEADER")

                _set_header_text(self)
            else:
                file.seek(file_seek)
            header = strip_arr(header_text.split(';'))
            header = [h.upper() for h in header]
            batch_size = 1000
            while True:
                print_flush(f"\rPopulating from file '{file_name}' ({year}): "
                            f"{format_file_size(entry['file_seek'])} / {format_file_size(file_size)} "
                            f"({entry['file_seek'] / file_size:.2%})", end="")
                end = False
                rows = []
                for i in range(batch_size):
                    line = []
                    prev_line_text = ""
                    while True:
                        line_text = prev_line_text + file.readline().strip()
                        if not line_text:
                            end = True
                            break
                        line = [strip(l) for l in line_text.rstrip().split(';')]
                        if len(line) == len(header):
                            break
                        prev_line_text = line_text
                    if end:
                        break
                    row = {}
                    for h, v in zip(header, line):
                        row[h] = v
                    row["year"] = year
                    rows.append(row)

                @db_session
                def _insert_rows(session, db):
                    aux_collection = self.get_aux_collection(session, db)
                    target_collection = self.get_target_collection(session, db)
                    DbOperation(session, db).insert_data(target_collection, rows, "INSERT ROWS")

                    if end:
                        print_flush(f"\r\x1b[1K\rPopulating from file '{file_name}' ({year}): {' ' * 35}", end="")
                        print_flush(f"\r\x1b[1K\rPopulating from file '{file_name}' ({year}): done!")
                        DbOperation(session, db).delete_data(aux_collection, entry, "DROP AUX COLLECTION")
                    else:
                        entry["file_seek"] = file.tell()
                        entry["tr_id"] = entry["tr_id"] + 1
                        DbOperation(session, db).insert_data(aux_collection, [{
                            "file_name": file_name,
                            "year": year,
                            "file_seek": file.tell(),
                            "header": header_text,
                            "tr_id": entry["tr_id"]}], "UPDATE FILE SEEK")

                _insert_rows(self)

                @db_session
                def _remove_old_aux(session, db):
                    aux_collection = self.get_aux_collection(session, db)
                    for old_entry in aux_collection.find(
                            ({"file_name": file_name} if end else
                            {"file_name": file_name, "tr_id": {"$ne": entry["tr_id"]}})):
                        DbOperation(session, db).delete_data(aux_collection, old_entry)

                _remove_old_aux(self)
                if end:
                    return self.start()

    def prepare(self):
        df_changed = False
        for file, year in self.fs.data_files:
            if get_file_encoding(file) == "":
                print_flush(f"Determining encoding of file '{file}': ", end="")
                read_file(file)
                print_flush("done!")
                df_changed = True
        if df_changed:
            self.fs.disconnect()
            self.fs.connect()

        @db_session
        def _prepare(session, db):
            aux_collection = self.get_aux_collection(session, db)
            if len(self.fs.data_files) == 0:
                return
            DbOperation(session, db).insert_data(
                aux_collection,
                [{
                    "file_name": file,
                    "year": year,
                    "file_seek": 0,
                    "header": "",
                    "tr_id": 0}
                    for file, year in self.fs.data_files],
                "FILL AUX COLLECTION")
            return False

        return _prepare(self)

    def do_query(self):
        @db_session
        def _do_query(session, db):
            target_collection = self.get_target_collection(session, db)
            return
        return _do_query(self)
