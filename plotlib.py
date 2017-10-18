import pandas as pd
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

from bokeh.io import hplot, output_file, show
from bokeh.plotting  import figure

from IPython.core.display  import display, HTML
display(HTML("<style>.container { width:100% !important; }</style>"))
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
    s = len(processing_time[1])
    for i in xrange(s):
        temp = throughin_111[1][i]/processing_time[1][i]
        processing_records_per_second_per_batch.append(temp)
            
    return (processing_time[0],processing_records_per_second_per_batch)




