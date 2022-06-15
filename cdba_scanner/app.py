import sys

import typer
from PyInquirer import prompt
from scanner.src.context import threadManager, messageHelper
from scanner.src.library.helper.validate_helper import ValidateHelper

from cdba_scanner.src.enum.general import OperationModeInput, TaskTableColumns
from cdba_scanner.src.service.task_mapping_service import TaskMappingService
from cdba_scanner.src.validator.path_validator import PathValidator


def main():
    operationModeQuestions = [
        {
            'type': 'list',
            'name': 'operationMode',
            'message': 'Operation Mode',
            'choices': [
                OperationModeInput.mapResultToTaskName.value,
            ],
        },
    ]
    mapResultToTaskNameQuestions = [
        {
            'type': 'input',
            'name': 'scanResultFilePath',
            'message': 'Scan Result File',
            'validate': PathValidator,
        },
        {
            'type': 'input',
            'name': 'taskTableFilePath',
            'message': 'Task List File',
            'validate': PathValidator,
        },
        {
            'type': 'input',
            'name': TaskTableColumns.task,
            'message': 'Task Column Name',
            'default': TaskTableColumns.task.value,
            'validate': ValidateHelper.notEmpty,
        },
        {
            'type': 'input',
            'name': TaskTableColumns.process,
            'message': 'Process Column Name',
            'default': TaskTableColumns.process.value,
            'validate': ValidateHelper.notEmpty,
        },
        {
            'type': 'input',
            'name': TaskTableColumns.developer,
            'message': 'Developer Column Name',
            'default': TaskTableColumns.developer.value,
            'validate': ValidateHelper.notEmpty,
        },
        {
            'type': 'input',
            'name': TaskTableColumns.inactive,
            'message': 'Inactive Column Name',
            'default': TaskTableColumns.inactive.value,
            'validate': ValidateHelper.notEmpty,
        },
    ]

    answers = prompt(operationModeQuestions)
    if len(answers) == 0:
        threadManager.setExit()
        sys.exit(1)

    if answers['operationMode'] == OperationModeInput.mapResultToTaskName.value:
        answers = prompt(mapResultToTaskNameQuestions)
        if len(answers) == 0:
            threadManager.setExit()
            sys.exit(1)
        columnMapping = {
            TaskTableColumns.task: answers[TaskTableColumns.task],
            TaskTableColumns.process: answers[TaskTableColumns.process],
            TaskTableColumns.developer: answers[TaskTableColumns.developer],
            TaskTableColumns.inactive: answers[TaskTableColumns.inactive],
        }
        scanResultFilePath = answers['scanResultFilePath']
        taskTableFilePath = answers['taskTableFilePath']

        mappingService = TaskMappingService()
        mappingService.mapFolderToTask(columnMapping, scanResultFilePath, taskTableFilePath)

    debug = False

    if debug:
        messageHelper.print(f'Debug Info:')

    threadManager.setExit()


if __name__ == "__main__":
    # launch main program
    typer.run(main)
