from cdba_scanner.src.exception.app_exception import AppException


class FileException(AppException):
    pass


class FileFormatException(FileException):
    pass


class PathNotFoundException(FileException):
    pass
