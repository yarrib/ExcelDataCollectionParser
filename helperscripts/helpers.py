import pandas as pd
import datetime as dt
import re
import glob
from IPython.display import display
from pandas.core.base import DataError
from xlrd import XLRDError
import numpy as np

def excel_extractor(file, *args):
    """utility function that takes in an excel file, parses a selection of its 
    sheets if named by month into a list of dataframes

    Args:
        file (Microsoft Excel File): [Any micrsoft Excel 2007 file (.xlsx) 
        containing data in a quasi tabular format with given delineators, sheets by month names]

    Returns:
        [list]: [a list of pandas dataframes]
    """
    wb = pd.ExcelFile(file)
    valid_months = ['May','June', 'July', 'Aug', 'September', 'October']
    used_months = list(set(wb.sheet_names) & set(valid_months))
    dfs = []
    for idx,m in enumerate(used_months):
        dfs.append(wb.parse(m))
    return dfs
    
# ---------------------------------------------------------------------------
def tabularize_df(df: pd.DataFrame, big_num: int, *args) -> pd.DataFrame:
    """ingests a pandas dataframe

    Args:
        df ([Pandas Dataframe]): [Dataframe containing value counts by named categories:  
            categories = ['Athletic Fields','Garden','Hard Court Surfaces', 'Picnic Areas',
            'Playground/Tot Lot','Trails/Paths','Wading Pool'] in need of restructuring]
        big_num ([int]): [large integer used to avoid DataError and ValueError in parsing poor quality data]

    Returns:
        [Pandas Dataframe]: [a tidy dataframe object after some preprocessing]
    """
    # get our year from the top part of the excel sheet
    current_year = str(df.columns[0].split(" ")[-1])
    # iterate through dataframe rows because bad formatting,
    # then add each row into a value list with the index as the key for the dict
    these_rows = {}
    for idx, row in df.iterrows():
        addList = row.tolist()
        # enforce a list length of 16 vals
        these_rows[idx] = addList[:16]
    
    # ---------using cats as defined, parse each row    
    categories = ['Athletic Fields','Garden','Hard Court Surfaces', 'Picnic Areas','Playground/Tot Lot','Trails/Paths','Wading Pool']
    value_dict = {}
    current_date = ""
    for row in these_rows:
        # take action based on first value in the col
        flag = str(these_rows[row][0])
        #print(these_rows[row])
        
        # want to ignore the summary column
        if not re.match('.* SUMMARY', flag):
            # handling primary format for dates
            if re.match('Date.*', flag):
                #print(f'date flag: {flag}')
                # set our date and init a dict
                if ":" in flag:
                    current_date = str(flag).split(": ")[1]
                else: # sometimes we don't have the same date format of (e.g.) "Date: June 10", something could be "Date June 10"
                    current_date = str(flag).replace("Date ", "")
                #print(current_date)
                value_dict[current_date] = []
            elif 'Location' in flag:
                # look for a datetime value in the list
                for value in these_rows[row]:
                    if type(value) == dt.datetime:
                        current_date = value.strftime("%b %d")
                        current_year = value.strftime("%Y")
                        value_dict[current_date] = []
            elif flag in categories and current_date != "SKIP":
                # so we dont count the summary numbers and end up with a double count, set SKIP condition
                # otherwise append values if we have a categorical count row id'd
                try:
                    # we use a naive method to create the dates here because of the primary date format. 
                    # by doing this we do (in the previous elif) run the risk of un-datetiming a datetime just to 
                    # manipulate it later, but it is intentional because of the inconsistencies 
                    value_dict[current_date].append([current_date + " "+ current_year] + these_rows[row]) 
                except KeyError:
                    print(current_date,current_year)
                    break
        else:
            # ignore things like the 'summary' 
            current_date = 'SKIP'

    # flatten the row array to the levels of the lists, basically the dict is too nested
    # give our column names explicitly
    flattened = [row for array in value_dict.values() for row in array]

    # convert any nans to 0 numeric in the list before assignment to dataframe because of hte assertion error
    for idx, sublist in enumerate(flattened):
        # handle nans - leverage the big_num here to avoid making a nan into a misleading 0 for count
        flattened[idx] = [big_num if str(x)=='nan' else x for x in sublist]

    df_new = pd.DataFrame(data=flattened,columns = [
        'Date', 'Amenity', 'blah', '9am', '10am', '11am', '12pm',
        '1pm', '2pm', '3pm','4pm', '5pm', '6pm', '7pm', '8pm', '9pm', '10pm'])
    
    df_new.drop('blah', axis = 1, inplace = True)

    # handle string value X as inputted for some reason, 
    # again leverage the big_num here to avoid adding 0 when the count isn't really 0
    df_new.iloc[:,2:] = df_new.iloc[:,2:].apply(lambda x: x.replace("X", big_num)) 

    return df_new


# ---------------------------------------------------------------------------
def restructure_dataframe(df, file, big_num, *args):
    """restructures a partially fixed dataframe by handling some column type mismatches, cleanup, melt/pivot/reshape 
    organize columns, add source file name details so as to identify the source file for data validation (manual step by
    others), and create a datetime column by date/hour of day

    Args:
        df ([pandas dataframe]): [partiall prepared dataframe, generally accepted to be output from the tabularize_df func]
        file ([str]): [string value created elsewhere which serves as some sort of identifier to be added as a static column value
            based on the file source location and name]
        big_num ([int]): [large integer used to avoid DataError and ValueError in parsing poor quality data]

    Returns:
        [pandas dataframe]: [an even tidier dataframe with contextual information (datetime column, source data col)]
    """
    # handle any column values expected to be numeric that are not, replace them with NaN (effectively - by coercing)
    df.iloc[:,2:] = df.iloc[:,2:].apply(pd.to_numeric, errors='coerce')
    # ----------------------
    # now trasnform on the axis for date and amenity, resulting in a long df
    # then, pivot so date-time dceome one column and drop the multiindex
    df = df.melt(id_vars=['Date', 'Amenity'],var_name='Time', value_name='Count')
    df = df.pivot_table(index=['Date', 'Time'], columns = 'Amenity')
    df.columns = df.columns.map(lambda x: x[1] if x[1] != "" else x)
    df = df.reset_index()
                                                
    # now replace the big dummy value with np.nan so we don't have it or any misleading 0 value in a cell
    df.iloc[:,2:] = df.iloc[:,2:].replace(big_num, np.nan)

    # create the dateteime col and insert the filename col
    df_timed = df.copy()
    df_timed.insert(0, 'ReportedDateTime',df_timed['Date'] + " " + df_timed['Time'])
    df_timed.insert(0, 'SrcFileName', file)
    df_timed.drop(['Date', 'Time'], axis=1, inplace=  True)
    # need to handle invalid dates like Spetember 31 (LOL!)
    # turns out datetime lib doesn't like dates which aren't of this world...
    df_timed['ReportedDateTime'] = pd.to_datetime(df_timed['ReportedDateTime'], errors = 'coerce')
    # drop any non-valid dates
    df_timed.dropna(subset=['ReportedDateTime'], inplace=True)

    # remove bad count data from dataframe
    df_timed = df_timed[df_timed.iloc[:,2:].sum(axis=1) > 0]


    # a dataframe that is tidy
    return df_timed
        
    

# ---------------------------------------------------------------------------
def get_filenames(start_path, look_in_path, subdir, *args):
    """retrieval function for all the files (recursive) leveraging glob

    Args:
        start_path ([str]): [string value of the start directory (of the script running)]
        look_in_path ([str]): [the subdirectory (1 level down child) of the raw data]
        subdir ([str]): [subdirectory - generally the current variable in a loop]

    Returns:
        [list, list]: [list of qualified paths, list of identifier names for passing to 
        another function like restructure_dataframe]
    """
    file_list = glob.glob(f"{start_path}/{look_in_path}/{subdir}/*.xlsx")

    # handle the weird ~$ file thats a hidden/sys backup thing
    for y in file_list:
        if '~$' in str(y): # or str(y).find(r'~$'):
            file_list.remove(y)

    out_names = []
    for f in file_list:
        stopwords = ['summer', 'facility', 'usage', 'stats', 'outdoor']
        f = f.split("\\")[-1]
        # tidy up the extra path data
        f = re.sub("Users/.*/Documents/", "", f)
        f = f.replace("Stats","").replace("xlsx","").split()
        f = " ".join([x for x in f if x.lower() not in stopwords])
        pattern = re.compile('[\W_]+')
        f2 = pattern.sub('_', f)
        out_names.append(f2)
    
    return file_list, out_names


# ---------------------------------------------------------------------------
def iter_transform_count_files(start_path, look_in_path, subdir, write_path, file_prepend, *args):
    """[iterative function to go through all files in a subdirectory and apply functions to extract data to csvs]

    Args:
        start_path ([str]): [string value of the start directory (of the script running)]
        look_in_path ([str]): [the subdirectory (1 level down child) of the raw data]
        subdir ([str]): [subdirectory - generally the current variable in a loop]
        write_path ([str]): [the output location relative to the file running this function]
        file_prepend ([str]): [string to prepend as the file name - some string concat of the path identifier probably]
    """
    # get all files in the directory
    these_files, these_names = get_filenames(start_path, look_in_path, subdir)
    # itereate and apply functions to parse data
    for i, f in enumerate(these_files):
        f2 = re.sub("Users/.*/Documents/", "", f)
        print(f"trying...{f2}")
        # extract valid dataframes
        try:
            dfs = excel_extractor(f)
        except XLRDError:
            print('encountered XLRDError...skipping file...')
            pass
        # set big_num to pass in to handle "X", 0's and so on
        big_num = 9999999999
        prepared_dfs = []
        for d in dfs:
            try:
                prepared_dfs.append(tabularize_df(d, big_num))
            except DataError:
                print('encountered a pandas DataError, skipping...')
                continue
        # iterate over the prepared dfs and save them if we have data
        for j, prep_df in enumerate(prepared_dfs):
            if prep_df.shape[0] > 0:
                prep_df = restructure_dataframe(prep_df,these_names[i], big_num)
                prep_df.to_csv(f"{start_path}/{write_path}/{file_prepend}_{these_names[i]}{j+5}.csv", index = False)
            else:
                print(f'no records in month {j+5} for {these_names[i]}')
        print(f"done with...{f2}")