"""
Excel operations
It includes;
  -getSheetNames : read sheet names from excel file
  -writeExcelData ; write any data to excel file.
        x:          Dataframe
        excelfile:  your excel file path
        startrow: row index
        startcol:column index
        reshape: your choices whether vertical or horizontal
  -readExcelSheet1
        excelfile:excel path
  -readExcelRange
          x:          Dataframe
        excelfile:  your excel file path
        startrow: row index
        endrow:   end index
        startcol:column index
        endcol; end column index
        reshape: your choices whether vertical or horizontal
  -readExcel
"""


def getSheetNames(excelfile):
    from pandas import ExcelFile
    return (ExcelFile(excelfile)).sheet_names


def writeExcelData(x, excelfile, sheetname, startrow, startcol, reshape=None):
    from pandas import DataFrame, ExcelWriter
    from openpyxl import load_workbook
    if reshape:
        df = DataFrame(x.reshape(-1, len(x)))
    else:
        df = DataFrame(x)
    book = load_workbook(excelfile, keep_vba=True)
    writer = ExcelWriter(excelfile, engine='openpyxl')
    writer.book = book
    writer.sheets = dict((ws.title, ws) for ws in book.worksheets)
    df.to_excel(writer, sheet_name=sheetname, startrow=startrow - 1, startcol=startcol - 1, header=False, index=False)
    writer.save()
    writer.close()


def readExcelSheet1(excelfile):
    from pandas import read_excel
    return (read_excel(excelfile)).values


def readExcelRange(excelfile, sheetname="Sheet1", startrow=1, endrow=1, startcol=1, endcol=1):
    from pandas import read_excel
    values = (read_excel(excelfile, sheetname, header=None)).values;
    return values[startrow - 1:endrow, startcol - 1:endcol]


def readExcel(excelfile, **args):
    if args:
        data = readExcelRange(excelfile, **args)
    else:
        data = readExcelSheet1(excelfile)
    if data.shape == (1, 1):
        return data[0, 0]
    elif (data.shape)[0] == 1:
        return data[0]
    else:
        return data
