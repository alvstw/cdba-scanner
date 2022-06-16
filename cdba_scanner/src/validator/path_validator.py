from os import path

from prompt_toolkit.validation import Validator, ValidationError


class FileValidator(Validator):
    def validate(self, document):
        if not path.exists(document.text):
            raise ValidationError(message='The path does not exist',
                                  cursor_position=len(document.text))
        if not path.isfile(document.text):
            raise ValidationError(message='The path is not a file',
                                  cursor_position=len(document.text))
