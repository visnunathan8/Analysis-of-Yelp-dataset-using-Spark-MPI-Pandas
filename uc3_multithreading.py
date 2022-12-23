from threading import Thread
import time
import pandas as pd

dataset = r'E:\d1.csv'
column_names = ['business_name', 'review_count', 'categories', 'city', 'state']
df_final = pd.DataFrame(columns=column_names)
num_of_threads=0


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



def task(skip, read):
    if skip==0:
        skip=1
    columns=list(pd.read_csv(dataset, skiprows=0, nrows=0).columns)
    df = pd.read_csv(dataset, skiprows=skip, nrows=read,names=columns)
    result = chunk_query(df)
    global df_final 
    df_final= pd.concat([df_final, result])
    

def Input_Thread_Count():
    global num_of_threads
    num_of_threads = int(input("Enter the number of threads : "))


def Thread_function():
    thread_handle = []
    tot_rows = 460402
    to_read = round((tot_rows / num_of_threads)) + 1

    for j in range(0, num_of_threads):
        t = Thread(target=task, args=((to_read * j), to_read))
        thread_handle.append(t)
        t.start()

    for j in range(0, num_of_threads):
        thread_handle[j].join()
        

def time_taken(start_time,end_time,num_of_threads):
    total_time= f'{(end_time - start_time):.1f}'
    print(f'\n------------>  Time Taken by {num_of_threads} threads : {total_time} seconds\n')

if __name__ == '__main__':
    start_time = time.time()
    Input_Thread_Count()
    Thread_function()
    print(df_final)
    end_time = time.time()
    time_taken(start_time,end_time,num_of_threads)

