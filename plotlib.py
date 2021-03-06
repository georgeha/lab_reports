import pandas as pd
import time
import dateutil
import dateutil.parser
import os
import matplotlib.pyplot as plt
import numpy as np
pd.set_option('display.float_format', lambda x: '%.3f' % x)
from bokeh.plotting  import figure, output_file
from bokeh.charts  import Bar, output_file, show
import bokeh.plotting as bplt
from bokeh.palettes  import Dark2_5 as palette
from bokeh.models  import Label, Title
import itertools  
from bokeh.layouts  import gridplot,row,column

#from bokeh.io import hplot, output_file, show
from bokeh.plotting  import figure

#from IPython.core.display  import display, HTML
#display(HTML("<style>.container { width:100% !important; }</style>"))
from bokeh.io  import output_notebook
plt.style.use('ggplot')
output_notebook()




directory = '/home/georgeha/repos/midas_exps/streaming/k-means/spark/'
directory = directory + '1-node/'

def plot_producer(producer_filename,p_number,fig_count,txt,showPlot):
        rates =pd.read_csv(directory + producer_filename)
        x_values = rates['Num_Messages'].tolist()
        y_values = rates['KB/sec'].tolist()
                
        if showPlot==True:
            f = plt.figure()
            plt.xlim(0,12500)
            plt.plot(x_values,y_values)
            plt.ylabel('KB/s')
            plt.xlabel('Msg number')
            plt.title("Producer " + str(p_number)+ "  throughput")
            text = 'Figure ' + str(fig_count) + ' : ' + txt
            f.text(.20, .02, text, ha='center')
                                                
        return (x_values,y_values)



def plot_scheduling_and_Total_Delay(spark_metrics_dir,fig_count,title,showPlot):
        
        metrics = pd.read_csv(directory + spark_metrics_dir ,skipinitialspace=True)
        scheduling_delay = metrics['SchedulingDelay'].tolist()    
        TotalDelay = metrics['TotalDelay'].tolist()
                
        scheduling_delay = map(lambda x: x/1000, scheduling_delay)  # convert sec from milisec
        TotalDelay = map(lambda x: x/1000, TotalDelay)
        
        processing_time = []
        for i in xrange(len(scheduling_delay)):
            el = TotalDelay[i] - scheduling_delay[i]
            processing_time.append(el)

        x_values = range(0,metrics['SchedulingDelay'].count())
        # create a new plot with a title and axis labels
        if showPlot==True:
            p = figure(title="Figure " + str(fig_count) + ": Spark Scheduling delay " + title, x_axis_label='miniBatch Number', y_axis_label='Delay in sec')
            # add a line renderer with legend and line thickness
            p.line(x_values, scheduling_delay, legend="Scheduling Delay", line_width=2,line_color="red")
            p.line(x_values,TotalDelay,legend="Total Delay", line_width=2)
            p.line(x_values,processing_time,legend="Processing Time", line_width=2,line_color='green') # in seconds
            #throughput = (TotalDelay - scheduling_delay)/metrics['NumberRecords']
            bplt.show(p)
            
        return (x_values,processing_time)



def plot_throughput_per_mini_batch(spark_metrics_dir,fig_count,showPlot):
        
        metrics = pd.read_csv(directory + spark_metrics_dir ,skipinitialspace=True)
        scheduling_delay = metrics['SchedulingDelay']
        TotalDelay = metrics['TotalDelay']
        x_values = range(0,scheduling_delay.count())    
        throughput = metrics['NumberRecords']     # check That
                            
        if showPlot==True:
        # create a new plot with a title and axis labels
            p = figure(title="Figure " + str(fig_count) + " : Spark throughput/mini batch  - batch=60sec", x_axis_label='miniBatch Number', y_axis_label='records/batch')
            # add a line renderer with legend and line thickness
            p.line(x_values, throughput.tolist(), legend="Throughput", line_width=2,line_color="green")
            # show the results
            bplt.show(p)
        
        return  (x_values,throughput.tolist())


def plot_throughput_and_Delays(spark_metrics_dir,fig_count):
        
        
    metrics = pd.read_csv(directory + spark_metrics_dir ,skipinitialspace=True)
    scheduling_delay = metrics['SchedulingDelay'].tolist()    
    TotalDelay = metrics['TotalDelay'].tolist()
                    
    scheduling_delay = map(lambda x: x/1000, scheduling_delay)  # convert sec from milisec
    TotalDelay = map(lambda x: x/1000, TotalDelay)
                                
    processing_time = []
    for i in xrange(len(scheduling_delay)):
        el = TotalDelay[i] - scheduling_delay[i]
        processing_time.append(el)

    x_values = range(0,metrics['SchedulingDelay'].count())
    # create a new plot with a title and axis labels
    p1 = figure(width=500, height=500,title="Figure " + str(fig_count) + ": Spark Scheduling delay " , x_axis_label='miniBatch Number', y_axis_label='Delay in sec')
    # add a line renderer with legend and line thickness
    p1.line(x_values, scheduling_delay, legend="Scheduling Delay", line_width=2,line_color="red")
    p1.line(x_values,TotalDelay,legend="Total Delay", line_width=2)
    p1.line(x_values,processing_time,legend="Processing Time", line_width=2,line_color='green') # in seconds
        
    scheduling_delay = metrics['SchedulingDelay']
    TotalDelay = metrics['TotalDelay']
    throughput = metrics['NumberRecords']    # check That
    # create a new plot with a title and axis labels
    p2 = figure(width=500, height=500,title="Figure " + str(fig_count) + " : Spark throughput/mini batch  - batch=60sec", x_axis_label='miniBatch Number', y_axis_label='records/sec')
    # add a line renderer with legend and line thickness
    p2.line(x_values, throughput.tolist(), legend="Throughput", line_width=2,line_color="green")
            
    # put all the plots in an HBox
    plots = hplot(p1, p2)

    # show the results
    bplt.show(plots)
    return


def plot_decays(spark_metrics_dir_1,title_1,spark_metrics_dir_2,title_2,fig_count):
        
    metrics = pd.read_csv(directory + spark_metrics_dir_1 ,skipinitialspace=True)
    scheduling_delay = metrics['SchedulingDelay'].tolist()    
    TotalDelay = metrics['TotalDelay'].tolist()
        
    scheduling_delay = map(lambda x: x/1000, scheduling_delay)  # convert sec from milisec
    TotalDelay = map(lambda x: x/1000, TotalDelay)
        
    processing_time = []
    for i in xrange(len(scheduling_delay)):
        el = TotalDelay[i] - scheduling_delay[i]
        processing_time.append(el)

    x_values = range(0,metrics['SchedulingDelay'].count())
    # create a new plot with a title and axis labels
    p1 = figure(title="Figure " + str(fig_count) + ": Spark Scheduling delay " + title_1, x_axis_label='miniBatch Number', y_axis_label='Delay in sec')
    p1.line(x_values, scheduling_delay, legend="Scheduling Delay", line_width=2,line_color="red")
    p1.line(x_values,TotalDelay,legend="Total Delay", line_width=2)
    p1.line(x_values,processing_time,legend="Processing Time", line_width=2,line_color='green') # in seconds
    ##### 2nd plot
        
    metrics = pd.read_csv(directory + spark_metrics_dir_2 ,skipinitialspace=True)
    scheduling_delay = metrics['SchedulingDelay'].tolist()    
    TotalDelay = metrics['TotalDelay'].tolist()
        
    scheduling_delay = map(lambda x: x/1000, scheduling_delay)  # convert sec from milisec
    TotalDelay = map(lambda x: x/1000, TotalDelay)
            
    processing_time = []
    for i in xrange(len(scheduling_delay)):
        el = TotalDelay[i] - scheduling_delay[i]
        processing_time.append(el)

    x_values = range(0,metrics['SchedulingDelay'].count())
    # create a new plot with a title and axis labels
    p2 = figure(title="Figure " + str(fig_count) + ": Spark Scheduling delay " + title_2, x_axis_label='miniBatch Number', y_axis_label='Delay in sec')
    p2.line(x_values, scheduling_delay, legend="Scheduling Delay", line_width=2,line_color="red")
    p2.line(x_values,TotalDelay,legend="Total Delay", line_width=2)
    p2.line(x_values,processing_time,legend="Processing Time", line_width=2,line_color='green') # in seconds
                    
    # put all the plots in an HBox
    plots = hplot(p1, p2)

    # show the results
    bplt.show(plots)
    return
   


def rec_per_sec(proces_time,throughin):
        
    processing_records_per_second_per_batch = []
    s = len(proces_time[1])
    for i in xrange(s):
        temp = throughin[1][i]/proces_time[1][i]
        processing_records_per_second_per_batch.append(temp)
            
    return (proces_time[0],processing_records_per_second_per_batch)


def find_total_consumer_throughin(spark_metrics,ttc):
    
    metrics = pd.read_csv(directory + spark_metrics ,skipinitialspace=True)
    Nrecords = metrics['NumberRecords'].sum()
    Nrecords = Nrecords/ttc

    return Nrecords  # number of records per second



def find_total_producer_throughput(adir,producers, ttc,showPlot,text):

    from operator import add

    rates = pd.read_csv(directory + adir + producers[0])
    size = len(rates['Num_Messages'].tolist())
    total_x = [0]*size
    total_y = [0]*size

    for producer in producers:
        rates = pd.read_csv(directory + adir +  producer)
        x_values = rates['Num_Messages'].tolist()
        total_x = map(add,total_x,x_values)
        y_values = rates['KB/sec'].tolist()
        total_y = map(add,total_y,y_values)

    # add plotting of producer
    if showPlot==True:
        f = plt.figure()
        #plt.xlim(0,30000)
        plt.plot(total_x,total_y)
        plt.ylabel('KB/s')
        plt.xlabel('Msg number')
        plt.title("Total Producer  throughput")
        #text = 'Figure ' + str(fig_count) + ' : ' + txt
        f.text(.20, .02, text, ha='center')

    #avg production rate:
    avg_production_rate = len(total_x)*len(producers)/ttc  # number_of_messages_per_producer*Nproducers/ttc
    
    return (avg_production_rate,total_x,total_y)


#adir = '/home/georgeha/repos/midas_exps/streaming/k-means/spark/1-node/8-1-16-1r/producers'

def find_producers_file(directory):
        
    dataset = []
    producers_dirs = os.listdir(directory)
    for producer in producers_dirs:
        try:
            files = os.listdir(directory + '/' + producer)
            for el in files:
                if 'stdout-' in el:
                    dataset.append(producer+ '/' + el)
        except:
            pass

    return dataset



#producers = '8-1-2-new-settings/producers/'
#adir = '/home/georgeha/repos/midas_exps/streaming/k-means/spark/1-node/8-1-2-new-settings/producers/'
#dataset =  pltme.find_producers_file(adir)

def producer_throughtput_per_mini_batch(adir,producers):
        
    from operator  import add
        
    first_pass = True
    for producer in producers:
        producer_data =  pd.read_csv(adir + producer)
        timestamps = producer_data['TimeStamp']
        timestamps_list = timestamps.tolist()
        sent_messages_count = 0
        message_list = []
        start_time  = time.mktime(dateutil.parser.parse(timestamps_list[0]).timetuple()) 
                
        for atime in timestamps_list:
            cur_time = time.mktime(dateutil.parser.parse(atime).timetuple()) 
            if abs(start_time - cur_time) > 60: 
                message_list.append(sent_messages_count)
                start_time = cur_time
                sent_messages_count = 0
            else:
                sent_messages_count+=1
                        
        ## I only create this list the first time, becasue I need to know the size of the list
        if first_pass:
            all_messages_list = [0]*len(message_list)
            first_pass=False
                            
        while len(all_messages_list) < len(message_list):
            all_messages_list.append(0)
                            
        while len(message_list) < len(all_messages_list):
            message_list.append(0)
                            
        all_messages_list = map(add,all_messages_list,message_list)
                                    
    mb_per_second = []
    for record in all_messages_list:
        mb = (record*8*5000*3/1024**2)/60   # (record*DoublePrecisionFloat_bytes*Total_points*dimensions_of_points/convertion_to_mbs)/window_size)
        mb_per_second.append(mb)
                            
    return (all_messages_list,mb_per_second)
                                            
