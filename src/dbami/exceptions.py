class DbamiError(Exception):
    pass


class MigrationError(DbamiError, ValueError):
    pass


class DirectionError(MigrationError):
    pass
