import re

import numpy as np
import pandas as pd
from pandas import DataFrame
from scanner.src import context
from scanner.src.constant.general import FilePath
from scanner.src.exception.file_exception import PathNotFoundException
from scanner.src.library.helper.file_helper import FileHelper
from scanner.src.library.helper.time_helper import TimeHelper
from thefuzz import process, fuzz

from cdba_scanner.src.enum.general import TaskTableColumns, MappedTaskFolderColumns, SearchResultColumns, FuzzyMatch


class TaskMappingService:
    def mapFolderToTask(self, columnMapping: dict, scanResultFilePath: str, taskTableFilePath: str):
        scanResultDf = TaskMappingService.readSearchResultFile(scanResultFilePath)
        taskTableDf = TaskMappingService.readTaskTableFile(taskTableFilePath, columnMapping)
        rs = TaskMappingService.createMappedTaskFolderDf(scanResultDf, taskTableDf)
        try:
            resultFilePath = TaskMappingService.writeCSV(rs)
            context.messageHelper.print(f'The mapped table is saved: {resultFilePath}')
        except Exception as err:
            context.messageHelper.print(f'Something went wrong while saving the mapped table: {err}')

    @staticmethod
    def readSearchResultFile(scanResultFilePath: str) -> DataFrame:
        try:
            df: DataFrame = pd.read_csv(scanResultFilePath)
            df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
            df[SearchResultColumns.extractedTaskName.value] = pd.NaT
            pattern = r'^\\\\.*?\\RPA\\(.*?)\\.*$'
            for i, row in df.iterrows():
                rs = re.search(pattern, row[SearchResultColumns.matchedFile.value])
                if rs:
                    matchedTask = rs.group(1)
                    df.iat[i, df.columns.get_loc(SearchResultColumns.extractedTaskName.value)] = matchedTask
            df = df[pd.notnull(df[SearchResultColumns.extractedTaskName.value])]
            return df
        except FileNotFoundError:
            raise PathNotFoundException
        except KeyError as err:
            context.messageHelper.print(str(err))
        except Exception as err:
            context.messageHelper.print(str(err))

    @staticmethod
    def readTaskTableFile(taskTableFilePath: str, columnMapping: dict) -> DataFrame:
        try:
            df: DataFrame = pd.read_csv(taskTableFilePath)
            df = df.loc[:, ~df.columns.str.contains('^Unnamed')]

            columnName = columnMapping.get(TaskTableColumns.task)
            if columnName is None or columnName not in df.columns:
                raise Exception

            columnName = columnMapping.get(TaskTableColumns.process)
            if columnName is None or columnName not in df.columns:
                raise Exception

            columnName = columnMapping.get(TaskTableColumns.developer)
            if columnName is None or columnName not in df.columns:
                raise Exception

            columnName = columnMapping.get(TaskTableColumns.inactive)
            if columnName is None or columnName not in df.columns:
                raise Exception

            df.rename(columns={columnMapping.get(TaskTableColumns.task): TaskTableColumns.task.value}, inplace=True)
            df.rename(columns={columnMapping.get(TaskTableColumns.process): TaskTableColumns.process.value},
                      inplace=True)
            df.rename(columns={columnMapping.get(TaskTableColumns.developer): TaskTableColumns.developer.value},
                      inplace=True)
            df.rename(columns={columnMapping.get(TaskTableColumns.inactive): TaskTableColumns.inactive.value},
                      inplace=True)

            df[[TaskTableColumns.inactive.value]] = df[[TaskTableColumns.inactive.value]].replace('Y', True)
            df[[TaskTableColumns.inactive.value]] = df[[TaskTableColumns.inactive.value]].fillna(False)
            df = df.fillna(np.nan).replace([np.nan], [None])  # Normalize with None
            return df
        except FileNotFoundError:
            raise PathNotFoundException
        except KeyError as err:
            context.messageHelper.print(str(err))
        except Exception as err:
            context.messageHelper.print(str(err))

    @staticmethod
    def createMappedTaskFolderDf(searchResultDf: DataFrame, taskTableDf: DataFrame) -> DataFrame:
        def getIndexInList(findValue: str, searchList: list) -> [int, None]:
            try:
                return searchList.index(findValue)
            except ValueError:
                return None

        colFolderName = MappedTaskFolderColumns.folderName.value
        colTask = MappedTaskFolderColumns.task.value
        colProcess = MappedTaskFolderColumns.process.value
        colDeveloper = MappedTaskFolderColumns.developer.value
        colInactive = MappedTaskFolderColumns.inactive.value
        colMappedBy = MappedTaskFolderColumns.mappedBy.value
        colConfident = MappedTaskFolderColumns.confident.value
        colFullPath = MappedTaskFolderColumns.fullPath.value

        rowList = []
        for i, row in searchResultDf.iterrows():
            n = int(i) - 1
            rowData = {}
            currentFolderName = row[SearchResultColumns.extractedTaskName.value]

            # Task Name Exact Match
            taskNameList = taskTableDf[TaskTableColumns.task.value].tolist()
            index = getIndexInList(currentFolderName, taskNameList)
            if index is not None:
                rowData[colFolderName] = searchResultDf[SearchResultColumns.extractedTaskName.value].values[n]
                rowData[colTask] = taskTableDf[TaskTableColumns.task.value].values[index]
                rowData[colProcess] = taskTableDf[TaskTableColumns.process.value].values[index]
                rowData[colDeveloper] = taskTableDf[TaskTableColumns.developer.value].values[index]
                rowData[colInactive] = taskTableDf[TaskTableColumns.inactive.value].values[index]
                rowData[colMappedBy] = 'taskName'
                rowData[colConfident] = 100.0
                rowData[colFullPath] = searchResultDf[SearchResultColumns.matchedFile.value].values[n]
                rowList.append(rowData)
                continue

            # Process Name Exact Match
            processNameList = taskTableDf[TaskTableColumns.process.value].tolist()
            index = getIndexInList(currentFolderName, processNameList)
            if index is not None:
                rowData[colFolderName] = searchResultDf[SearchResultColumns.extractedTaskName.value].values[n]
                rowData[colTask] = taskTableDf[TaskTableColumns.task.value].values[index]
                rowData[colProcess] = taskTableDf[TaskTableColumns.process.value].values[index]
                rowData[colDeveloper] = taskTableDf[TaskTableColumns.developer.value].values[index]
                rowData[colInactive] = taskTableDf[TaskTableColumns.inactive.value].values[index]
                rowData[colMappedBy] = 'processName'
                rowData[colConfident] = 100.0
                rowData[colFullPath] = searchResultDf[SearchResultColumns.matchedFile.value].values[n]
                rowList.append(rowData)
                continue

            # Fuzzy Match
            try:
                fuzzyTaskName, fuzzyTaskNameScore = process.extractOne(
                    searchResultDf[SearchResultColumns.extractedTaskName.value].values[n], taskNameList,
                    scorer=fuzz.partial_token_sort_ratio
                )
                fuzzyProcessName, fuzzyProcessNameScore = process.extractOne(
                    searchResultDf[SearchResultColumns.extractedTaskName.value].values[n], processNameList,
                    scorer=fuzz.partial_token_sort_ratio
                )

                if fuzzyTaskNameScore > FuzzyMatch.threshold and fuzzyTaskNameScore > fuzzyProcessNameScore:
                    index = getIndexInList(fuzzyTaskName, taskNameList)
                    rowData[colFolderName] = searchResultDf[SearchResultColumns.extractedTaskName.value].values[n]
                    rowData[colTask] = taskTableDf[TaskTableColumns.task.value].values[index]
                    rowData[colProcess] = taskTableDf[TaskTableColumns.process.value].values[index]
                    rowData[colDeveloper] = taskTableDf[TaskTableColumns.developer.value].values[index]
                    rowData[colInactive] = taskTableDf[TaskTableColumns.inactive.value].values[index]
                    rowData[colMappedBy] = 'taskName'
                    rowData[colConfident] = fuzzyTaskNameScore
                    rowData[colFullPath] = searchResultDf[SearchResultColumns.matchedFile.value].values[n]
                    rowList.append(rowData)
                    continue

                if fuzzyProcessNameScore > FuzzyMatch.threshold and fuzzyProcessNameScore > fuzzyTaskNameScore:
                    index = getIndexInList(fuzzyProcessName, processNameList)
                    rowData[colFolderName] = searchResultDf[SearchResultColumns.extractedTaskName.value].values[n]
                    rowData[colTask] = taskTableDf[TaskTableColumns.task.value].values[index]
                    rowData[colProcess] = taskTableDf[TaskTableColumns.process.value].values[index]
                    rowData[colDeveloper] = taskTableDf[TaskTableColumns.developer.value].values[index]
                    rowData[colInactive] = taskTableDf[TaskTableColumns.inactive.value].values[index]
                    rowData[colMappedBy] = 'processName'
                    rowData[colConfident] = fuzzyProcessNameScore
                    rowData[colFullPath] = searchResultDf[SearchResultColumns.matchedFile.value].values[n]
                    rowList.append(rowData)
                    continue
            except Exception as err:
                context.messageHelper.print(f'There was a problem while performing fuzzy match: {err}')

            # No match
            rowData[colFolderName] = searchResultDf[SearchResultColumns.extractedTaskName.value].values[n]
            rowData[colTask] = None
            rowData[colProcess] = None
            rowData[colDeveloper] = None
            rowData[colInactive] = None
            rowData[colMappedBy] = None
            rowData[colFullPath] = searchResultDf[SearchResultColumns.matchedFile.value].values[n]
            rowList.append(rowData)

        df = DataFrame(rowList)
        return df

    @staticmethod
    def writeCSV(df: DataFrame) -> str:
        filePath = FileHelper.joinPath(FilePath.DATA, TimeHelper.formatTime(fmt='%Y-%m-%d %H%M'))
        filePath = f'{filePath}.csv'
        df.to_csv(filePath)

        return filePath
