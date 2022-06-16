import sys
from typing import List

import typer
from PyInquirer import prompt
from scanner.src.constant.fileType import OtherFileType, ExcelFileType, RpaFileType, AllFileType
from scanner.src.context import threadManager, messageHelper
from scanner.src.library.helper.data_helper import DataHelper
from scanner.src.library.helper.file_helper import FileHelper
from scanner.src.library.helper.validate_helper import ValidateHelper
from scanner.src.report_service import ReportService
from scanner.src.search_service import SearchService
from scanner.src.validator.console.positive_number_validator import PositiveNumberValidator

from cdba_scanner.src.enum.general import OperationModeInput, TaskTableColumns
from cdba_scanner.src.service.task_mapping_service import TaskMappingService
from cdba_scanner.src.validator.path_validator import FileValidator


def main():
    operationModeQuestions = [
        {
            'type': 'list',
            'name': 'operationMode',
            'message': 'Operation Mode',
            'choices': [
                OperationModeInput.mapResultToTaskName.value,
                OperationModeInput.contentScanning.value,
            ],
        },
    ]
    mapResultToTaskNameQuestions = [
        {
            'type': 'input',
            'name': 'scanResultFilePath',
            'message': 'Scan Result File',
            'validate': FileValidator,
        },
        {
            'type': 'input',
            'name': 'taskTableFilePath',
            'message': 'Task List File',
            'validate': FileValidator,
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
    contentScanningQuestions = [
        {
            'type': 'input',
            'name': 'scanDirectory',
            'message': 'Scan Directory',
            'validate': FileValidator
        },
        {
            'type': 'checkbox',
            'name': 'scanFileTypes',
            'message': 'Files Type',
            'choices': [
                {
                    'name': AllFileType.name,
                    'checked': True
                },
                {
                    'name': RpaFileType.name,
                },
                {
                    'name': ExcelFileType.name,
                    'disabled': 'Currently not supported'
                },
                {
                    'name': OtherFileType.name,
                }
            ],
        },
        {
            'type': 'input',
            'name': 'scanKeyword',
            'message': 'Scan Keyword',
            'validate': ValidateHelper.notEmpty
        },
        {
            'type': 'input',
            'name': 'exclusionRule',
            'message': 'Exclusion Rule',
            'validate': lambda value: value == '' or ValidateHelper.isValidRegex(value)
        },
        {
            'type': 'confirm',
            'name': 'caseSensitive',
            'message': 'Case Sensitive',
            'default': False
        },
        {
            'type': 'input',
            'name': 'scanDepth',
            'message': 'Scan Level (0 = search every depth)',
            'default': '0',
            'validate': PositiveNumberValidator,
            'filter': DataHelper.convertToInteger
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
    elif answers['operationMode'] == OperationModeInput.contentScanning.value:
        answers = prompt(contentScanningQuestions)

        if len(answers) == 0:
            threadManager.setExit()
            sys.exit(1)

        searchService: SearchService = SearchService()
        reportService: ReportService = ReportService()

        debug = False

        scanDirectory: str = answers['scanDirectory']
        scanFileTypes: List[str] = answers['scanFileTypes']
        scanKeyword: str = answers['scanKeyword']
        exclusionRule: str = answers['exclusionRule'] if answers['exclusionRule'] != '' else None
        caseSensitive: bool = answers['caseSensitive']
        scanDepth: [int, None] = answers['scanDepth'] if answers['scanDepth'] != 0 else None

        rs = searchService.searchKeyword(scanDirectory, scanKeyword, scanFileTypes=scanFileTypes,
                                         exclusionRule=exclusionRule, depth=scanDepth, caseSensitive=caseSensitive)
        csvName = reportService.writeCSV(rs)

        messageHelper.print(f'Saved result to: {csvName}')

        debug = False

        if debug:
            messageHelper.print(f'Debug Info:')
            messageHelper.print(f'FileHelper.isFile: {FileHelper.isFile.cache_info()}')
            messageHelper.print(f'FileHelper.isDirectory: {FileHelper.isDirectory.cache_info()}')
            messageHelper.print(f'FileHelper.listDirectory: {FileHelper.listDirectory.cache_info()}')

    threadManager.setExit()


if __name__ == "__main__":
    # launch main program
    typer.run(main)
