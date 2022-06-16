from enum import Enum


class FuzzyMatch:
    threshold = 75


class OperationModeInput(str, Enum):
    mapResultToTaskName = 'Map Scan Result to Task Name'


class SearchResultColumns(str, Enum):
    fullPath = 'FullPath'
    extractedTaskName = 'Extracted Task Name'


class TaskTableColumns(str, Enum):
    task = 'Task'
    process = 'ProcessName'
    developer = 'Developers'
    inactive = 'Inactive'


class MappedTaskFolderColumns(str, Enum):
    folderName = 'FolderName'
    task = 'Task'
    process = 'ProcessName'
    developer = 'Developers'
    inactive = 'Inactive'
    mappedBy = 'MappedBy'
    confidence = 'Confidence'
    fullPath = 'FullPath'
