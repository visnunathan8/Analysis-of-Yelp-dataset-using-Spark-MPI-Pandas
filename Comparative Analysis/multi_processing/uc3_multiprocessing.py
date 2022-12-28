import time
import pandas as pd
from multiprocessing import Pool
from collections import Counter
dataset = r'E:\d1.csv'


def complimentsum(row):
        col_sum = row['compliment_hot'] + row['compliment_more'] + row['compliment_profile'] + row['compliment_cute'] + row['compliment_list'] + row['compliment_note'] + row['compliment_plain'] + row['compliment_cool'] + row['compliment_funny'] + row['compliment_writer'] + row['compliment_photos']
        return col_sum


def chunk_query(df):
    uc3 = df[df['elite'] != '']
    uc3['total_compliments'] = uc3.apply(complimentsum, axis=1)
    uc3 = uc3[(uc3['fans'] > 100) | (uc3['review_count'] > 500) | (uc3['total_compliments'] > 1000)]
    uc3 = uc3[uc3['average_stars'] > 4.0]
    uc3 = uc3[['business_name', 'review_count', 'categories', 'city', 'state']]
    return uc3

def map_tasks(reading_info: list):
    columns = list(pd.read_csv(dataset, skiprows=0, nrows=0).columns)
    df = pd.read_csv(dataset, nrows=reading_info[0], skiprows=reading_info[1],names=columns)
    result=chunk_query(df)
    return result

def Input_Process_Count():
    global num_of_process
    num_of_process = int(input("Enter the number of process : "))
    return num_of_process

def compute_multiprocessing():
    def distribute_rows(n_rows: int, n_processes):
        reading_info = []
        skip_rows = 1
        reading_info.append([n_rows - skip_rows, skip_rows])
        skip_rows = n_rows

        for _ in range(1, n_processes - 1):
            reading_info.append([n_rows, skip_rows])
            skip_rows = skip_rows + n_rows

        reading_info.append([None, skip_rows])
        return reading_info

    def query(lst):
        column_names = ['business_name', 'review_count', 'categories', 'city', 'state'] 
        df_final = pd.DataFrame(columns=column_names)
        for i in lst:
            df_final=pd.concat([df_final, i])
        print(df_final)

    def time_taken(start_time, end_time, processes):
        total_time = f'{(end_time - start_time):.1f}'
        print(f'\n------------> Time Taken by {processes} processes : {total_time} seconds\n')



    processes = Input_Process_Count()
    p = Pool(processes=processes)
    tot_rows = 460402
    start_time = time.time()
    lst = p.map(map_tasks, distribute_rows(n_rows=(tot_rows//processes)+1, n_processes=processes))

    p.close()
    p.join()
    end_time = time.time()
    query(lst)
    time_taken(start_time, end_time, processes)



if __name__ == '__main__':
    compute_multiprocessing()
