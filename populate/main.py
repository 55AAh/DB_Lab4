from populate import Populate
from user import print_flush, use_env_files, ask_variants, ask_confirm


def main():
    print_flush("\n\n\n")
    print_flush("Populate script started\n")
    use_env_files()

    populate = Populate()
    with populate:
        while handle_state(populate):
            print_flush()

    print_flush("\nPopulate script stopped")


def handle_state(populate):
    state = populate.get_state()
    if state == "clear":
        while True:
            sel = ask_variants("Db is clear.\n", {
                "r": "reload state",
                "s": "start population",
                "e": "exit",
            })
            if sel == "r":
                return reload(populate)
            elif sel == "s":
                reload(populate)
                if populate.get_state() != state:
                    return True
                return start(populate)
            elif sel == "e":
                return False
            print_flush()
    elif state == "finished":
        while True:
            sel = ask_variants("Looks like db is populated.\n", {
                "r": "reload state",
                "q": "execute test query",
                "d": "drop db",
                "e": "exit",
            })
            if sel == "r":
                return reload(populate)
            elif sel == "q":
                reload(populate)
                if populate.get_state() != state:
                    return True
                populate.do_query()
            elif sel == "d":
                reload(populate)
                if populate.get_state() != state:
                    return True
                if ask_confirm():
                    reload(populate)
                    if populate.get_state() != state:
                        return True
                    return drop_finished(populate)
            elif sel == "e":
                return False
            print_flush()
    elif state == "interrupted":
        while True:
            sel = ask_variants("Looks like the population was interrupted.\n", {
                "r": "reload state",
                "c": "continue population",
                "f": "assume population was finished",
                "d": "drop db",
                "e": "exit"
            })
            if sel == "r":
                reload(populate)
                if populate.get_state() != state:
                    return True
                return reload(populate)
            elif sel == "c":
                reload(populate)
                if populate.get_state() != state:
                    return True
                return resume(populate)
            elif sel == "f":
                reload(populate)
                if populate.get_state() != state:
                    return True
                if ask_confirm():
                    reload(populate)
                    if populate.get_state() != state:
                        return True
                    return assume_finished(populate)
            elif sel == "d":
                reload(populate)
                if populate.get_state() != state:
                    return True
                if ask_confirm():
                    reload(populate)
                    if populate.get_state() != state:
                        return True
                    return drop_interrupted(populate)
            elif sel == "e":
                return False
            print_flush()
    return False


def reload(populate):
    populate.fs.disconnect()
    populate.fs.connect()
    return True


def start(populate):
    populate.prepare()
    populate.start()
    return False


def resume(populate):
    populate.start()
    return False


def assume_finished(populate):
    clear_artifacts(populate)
    return True


def drop_finished(populate):
    print_flush("Dropping...")
    populate.drop_target()
    return True


def clear_artifacts(populate):
    print_flush("Clearing artifacts...")
    populate.drop_aux()
    return True


def drop_interrupted(populate):
    drop_finished(populate)
    clear_artifacts(populate)
    return True


if __name__ == "__main__":
    main()
