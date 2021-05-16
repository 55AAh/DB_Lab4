import os

DATA_FOLDER = "data"
ENCODINGS = ["utf-8-sig", "cp1251", "utf-8"]


def read_file(path):
    b_enc, path_enc = os.path.splitext(os.path.splitext(path)[0])
    path_enc = path_enc[1:]
    guess_encodings = ENCODINGS
    if path_enc in ENCODINGS:
        guess_encodings = [path_enc] + [enc for enc in ENCODINGS if enc != path_enc]
    for encoding in guess_encodings:
        try:
            with open(path, "r", encoding=encoding) as f:
                lines = f.readlines()
            if encoding != path_enc:
                os.rename(path, f"{b_enc}.{encoding}.csv")
            return lines
        except UnicodeError:
            pass
    raise UnicodeError(f"Cannot decode file, tried encodings: {', '.join(ENCODINGS)}")


def get_file_encoding(path):
    return os.path.splitext(os.path.splitext(path)[0])[1][1:]


def get_file_size(path):
    return os.path.getsize(path)


def format_file_size(b):
    if b < 1024:
        return f"{b} B"
    b /= 1024
    if b < 1024:
        return f"{b:.1f} KB"
    b /= 1024
    if b < 1024:
        return f"{b:.1f} MB"
    b /= 1024
    return f"{b:.1f} GB"


def parse_year(filename):
    fn = ""
    for c in filename:
        if c.isdigit():
            fn += c
        else:
            fn += " "
    numbers = [n for n in fn.split(' ') if len(n) > 0]
    if len(numbers) != 1 or len(numbers[0]) != 4:
        return None
    return int(numbers[0])


def get_datafiles_list(path):
    data_files, data_files_years = [], dict()
    for file in os.listdir(path):
        if file.endswith(".csv"):
            year = parse_year(os.path.splitext(os.path.splitext(file)[0])[0])
            if year is None:
                data_files.append(os.path.join(path, file))
            else:
                data_files_years[os.path.join(path, file)] = year
    return [kv for kv in sorted(data_files_years.items(), key=lambda kv: kv[1])] + \
           [(file, None) for file in data_files]


def strip(text):
    return text.strip("'\n\" ")


def strip_arr(arr):
    return map(strip, arr)
