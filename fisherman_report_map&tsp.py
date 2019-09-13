"""
Created on Fri Mar 15 13:09:10 2019



@author: leizhao
"""
import matplotlib.pyplot as plt
import numpy as np
from datetime import datetime,timedelta
import zlconversions as zl
import time
import os
import conda
conda_file_dir = conda.__file__
conda_dir = conda_file_dir.split('lib')[0]
proj_lib = os.path.join(os.path.join(conda_dir, 'share'), 'proj')
os.environ["PROJ_LIB"] = proj_lib
from mpl_toolkits.basemap import Basemap
import sys
import pandas as pd
import json

def check_time(df,time_header,start_time,end_time):
    '''keep the type of time is datetime
    input start time and end time, return the data between start time and end time'''
    for i in range(len(df)):
        if type(df[time_header][i])==str:
            df[time_header][i]=datetime.strptime(df[time_header][i],'%Y-%m-%d %H:%M:%S')
        if start_time<=df[time_header][i]<=end_time:
            continue
        else:
            df=df.drop(i)
    df=df.dropna()
    df.index=range(len(df))
    return df


def check_depth(df,mindepth):
    '''keep the depth is out of mindepth and correct the format of depth for example:-20 '''
    if len(df)>0:  
        for i in df.index:
            if abs(df['depth'][i])<abs(mindepth):
                df=df.drop(i)
        df.index=range(len(df))
    else:
        return df
    for i in range(len(df)):
        if df['depth'][i]>0:
            df['depth'][i]=-1*df['depth'][i]
    df=df.dropna()
    df.index=range(len(df))
    return df


def plot(df,ax1,ax2,linewidth=2,linestyle='--',color='y',alpha=0.5,label='Observed',marker='d',markerfacecolor='y',**kwargs):
    """import dataframe and ax, plot time-depth and time-temperature"""
    try:
        if len(df)>0:  #the length of dataframe must be bigger than 0
            ax1.plot_date(df['time'],df['temp'],linewidth=linewidth,linestyle=linestyle,color=color,alpha=alpha,label=label,marker=marker,markerfacecolor=markerfacecolor)
            ax2.plot_date(df['time'],df['depth'],linewidth=linewidth,linestyle=linestyle,color=color,alpha=alpha,label=label,marker=marker,markerfacecolor=markerfacecolor)
        max_t,min_t=np.nanmax(df['temp'].values),np.nanmin(df['temp'].values)
        max_d,min_d=np.nanmax(df['depth'].values),np.nanmin(df['depth'].values) 
    except:
        max_t,min_t,max_d,min_d=-9999,9999,-99999,99999 
    return  max_t,min_t,max_d,min_d
def draw_time_series_plot(dict,name,dtime,path_picture_save,timeinterval=30,dpi=300,mindepth=10):  
    """
    import the dictionary, this dictionary have data about Doppio,GoMOLFs,FVCOM and telemetered or this dict from create_obs_dpo_gmf_dict.py
    the unit of time interval is days
    use to draw time series plot """
    #get the latest time of get data, and back 30 days as start time
    df=pd.DataFrame.from_dict(dict[name])
#    df[]
    df['time']=df.index
    tele_df=df[['time','lat','lon','observation_T', 'observation_H']]
    tele_df.rename(columns={'observation_T':'temp','observation_H':'depth'},inplace=True)
    Doppio_df=df[['time','lat','lon','Doppio_T', 'Doppio_H']]
    Doppio_df.rename(columns={'Doppio_T':'temp','Doppio_H':'depth'},inplace=True)
    GoMOLFs_df=df[['time','lat','lon','GoMOLFs_T', 'GoMOLFs_H']]
    GoMOLFs_df.rename(columns={'GoMOLFs_T':'temp','GoMOLFs_H':'depth'},inplace=True)
    FVCOM_df=df[['time','lat','lon','FVCOM_T', 'FVCOM_H']]
    FVCOM_df.rename(columns={'FVCOM_T':'temp','FVCOM_H':'depth'},inplace=True)
    Clim_df=df[['time','lat','lon','Clim_T', 'NGDC_H']]
    Clim_df.rename(columns={'Clim_T':'temp','NGDC_H':'depth'},inplace=True)
    
    
    
    #through the parameter of mindepth to screen the data, make sure the depth is out of ten
    tele_df=check_depth(df=tele_df,mindepth=mindepth) #this dataframe is obervasion data
    Doppio_df=check_depth(df=Doppio_df,mindepth=mindepth)
    GoMOLFs_df=check_depth(df=GoMOLFs_df,mindepth=mindepth)
    FVCOM_df=check_depth(df=FVCOM_df,mindepth=mindepth)
    Clim_df=check_depth(df=Clim_df,mindepth=mindepth)
    #make sure the range of time through the interval and the last time
    if len(tele_df)==0:  
        print(name+': no valuable data')
        return 0
    endtime=tele_df['time'][len(tele_df)-1]
    if type(endtime)==str:
        endtime=datetime.strptime(endtime,'%Y-%m-%d %H:%M:%S')
    if dtime>endtime:
        start_time=endtime-timedelta(days=timeinterval)
    else:
        start_time=dtime-timedelta(days=timeinterval)
    #through the start time and end time screen data
    tele_dft=check_time(df=tele_df,time_header='time',start_time=start_time,end_time=dtime)
    Doppio_dft=check_time(df=Doppio_df,time_header='time',start_time=start_time,end_time=dtime)
    GoMOLFs_dft=check_time(df=GoMOLFs_df,time_header='time',start_time=start_time,end_time=dtime)
    FVCOM_dft=check_time(df=FVCOM_df,time_header='time',start_time=start_time,end_time=dtime)
    Clim_dft=check_time(df=Clim_df,time_header='time',start_time=start_time,end_time=dtime)
    #if boservation dataframe is no data,it will print the message that there is no data  and return zero.
    if len(tele_dft)==0:  
        print(name+': no valuable data')
        return 0
    #start to draw the picture.
    fig=plt.figure(figsize=(11.69,8.27))
    size=min(fig.get_size_inches())        
    fig.suptitle(name,fontsize=3*size, fontweight='bold')
    ax1=fig.add_axes([0.12, 0.52, 0.76,0.36])
    ax2=fig.add_axes([0.12, 0.12, 0.76,0.36])
    
    #parameter initialization
    Dmax_t,Dmin_t,Dmax_d,Dmin_d=-9999,9999,-99999,99999 
    Gmax_t,Gmin_t,Gmax_d,Gmin_d=-9999,9999,-99999,99999 
    Fmax_t,Fmin_t,Fmax_d,Fmin_d=-9999,9999,-99999,99999 
    
    #draw Graph for every module and get the minimum and maxmum of depth and temperature
    Tmax_t,Tmin_t,Tmax_d,Tmin_d=plot(df=tele_dft,ax1=ax1,ax2=ax2,linewidth=2,linestyle='-.',color='blue',alpha=0.5,label='Observed',marker='o',markerfacecolor='blue')
    Dmax_t,Dmin_t,Dmax_d,Dmin_d=plot(df=Doppio_dft,ax1=ax1,ax2=ax2,linewidth=2,linestyle='--',color='brown',alpha=0.5,label='DOPPIO',marker='^',markerfacecolor='brown')
    Gmax_t,Gmin_t,Gmax_d,Gmin_d=plot(df=GoMOLFs_dft,ax1=ax1,ax2=ax2,linewidth=2,linestyle='--',color='gray',alpha=0.5,label='GoMOLFs',marker='^',markerfacecolor='gray')
    Fmax_t,Fmin_t,Fmax_d,Fmin_d=plot(df=FVCOM_dft,ax1=ax1,ax2=ax2,linewidth=2,linestyle='--',color='black',alpha=0.5,label='FVCOM',marker='^',markerfacecolor='black')
    Cmax_t,Cmin_t,Cmax_d,Cmin_d=plot(df=Clim_dft,ax1=ax1,ax2=ax2,linewidth=2,linestyle='-',color='r',alpha=0.5,label='Clim',marker='d',markerfacecolor='r')
   
    #calculate the max and min of temperature and depth
    MAX_T=max(Tmax_t,Dmax_t,Gmax_t,Fmax_t,Cmax_t)
    MIN_T=min(Tmin_t,Dmin_t,Gmin_t,Fmin_t,Cmin_t)
    MAX_D=max(Tmax_d,Dmax_d,Gmax_d,Fmax_d,Cmax_d)
    MIN_D=min(Tmin_d,Dmin_d,Gmin_d,Fmin_d,Cmin_d)
    
    #calculate the limit value of depth and temperature 
    diff_temp=MAX_T-MIN_T
    diff_depth=MAX_D-MIN_D
    if diff_temp==0:
        textend_lim=0.1
    else:
        textend_lim=diff_temp/8.0
    if diff_depth==0:
        dextend_lim=0.1
    else:
        dextend_lim=diff_depth/8.0
    #the parts of label
    ax1.legend(prop={'size': 1.5*size})
    ax1.set_ylabel('Celsius',fontsize=2*size)  
    ax1.set_ylim(MIN_T-textend_lim,MAX_T+textend_lim)
    if len(tele_df)==1: #if there only one data, we need change the time limit as one week 
        ax1.set_xlim((tele_df['time'][0]-timedelta(days=3)),(tele_df['time'][0]+timedelta(days=4)))
    ax1.axes.get_xaxis().set_visible(False)
    ax1.tick_params(labelsize=1.5*size)
    ax12=ax1.twinx()
    ax12.set_ylabel('Fahrenheit',fontsize=2*size)
    #conversing the Celius to Fahrenheit
    ax12.set_ylim((MAX_T+textend_lim)*1.8+32,(MIN_T-textend_lim)*1.8+32)
    ax12.invert_yaxis()
    ax12.tick_params(labelsize=1.5*size)
    ax2.legend(prop={'size':1.5* size})
    ax2.set_ylabel('depth(m)',fontsize=2*size)
    ax2.set_ylim(MIN_D-dextend_lim,MAX_D+dextend_lim)
    if len(tele_df)==1:   #if there only one data, we need change the time limit as one week
        ax2.set_xlim(tele_df['time'][0]-timedelta(days=3),tele_df['time'][0]+timedelta(days=4))
    ax2.tick_params(labelsize=1.5*size)
    ax22=ax2.twinx()
    ax22.set_ylabel('depth(feet)',fontsize=2*size)
    ax22.set_ylim((MAX_D+dextend_lim)*3.28084,(MIN_D-dextend_lim)*3.28084)
    ax22.invert_yaxis()
    ax22.tick_params(labelsize=1.5*size)
    for tick in ax2.get_xticklabels():
        tick.set_rotation(350)
    
    #check the path is exist,if not exist,create it
    if not os.path.exists(path_picture_save+'/picture'+dtime.strftime('%Y-%m-%d')+'/'):
        os.makedirs(path_picture_save+'/picture'+dtime.strftime('%Y-%m-%d')+'/')
    plt.savefig(path_picture_save+'/picture'+dtime.strftime('%Y-%m-%d')+'/'+name+'_tsp_'+dtime.strftime('%Y-%m')+'obsclim.ps',dpi=dpi,orientation='landscape')
    print(name+' finished time series plot!')

def draw_map(df,name,dtime,path_picture_save,timeinterval=30,mindepth=10,dpi=300):
    """
    the type of start_time_local and end time_local is datetime.datetime
    use to draw the location of raw file and telemetered produced"""
    df=pd.DataFrame.from_dict(dict[i])
    df['time']=df.index
    df=df[['time','lat','lon','observation_T', 'observation_H']]
    df.rename(columns={'observation_T':'temp','observation_H':'depth'},inplace=True)
    #creat map
    #Create a blank canvas 
    df=check_depth(df.dropna(),mindepth=10) #screen out the data 
    #make sure the start time through the latest time of get data
    if len(df)==0:  #if the length of 
        print(name+': valuless data!')
        return 0
    endtime=df['time'][len(df)-1]
    if type(endtime)==str:
        endtime=datetime.strptime(endtime,'%Y-%m-%d %H:%M:%S')
    if dtime>endtime:
        start_time=endtime-timedelta(days=timeinterval)
    else:
        start_time=dtime-timedelta(days=timeinterval)
    df=check_time(df,'time',start_time,dtime) #screen out the valuable data that we need through the time
    if len(df)==0:  #if the length of 
        print(name+': valuless data!')
        return 0
    fig=plt.figure(figsize=(8,8.5))
    fig.suptitle('F/V '+name,fontsize=24, fontweight='bold')
 
    start_time=df['time'][0]
    end_time=df['time'][len(df)-1]
    if type(start_time)!=str:
        start_time=start_time.strftime('%Y/%m/%d')
        end_time=end_time.strftime('%Y/%m/%d')
    else:
        start_time=start_time.replace('-','/')[:10]
        end_time=end_time.replace('-','/')[:10]
    ax=fig.add_axes([0.03,0.03,0.85,0.85])
    ax.set_title(start_time+' to '+end_time)
    ax.axes.title.set_size(16)
    
    min_lat=min(df['lat'])
    max_lat=max(df['lat'])
    max_lon=max(df['lon'])
    min_lon=min(df['lon'])
    #keep the max_lon-min_lon>=0.2
    if (max_lon-min_lon)<=0.4: #0.2 is a parameter that avoid the dataframe only have one value.
        max_lon=max_lon+(0.4-(max_lon-min_lon))/2.0
        min_lon=max_lon-0.4
    #adjust the max and min,let map have the same width and height 
    if (max_lon-min_lon)>(max_lat-min_lat):
        max_lat=max_lat+((max_lon-min_lon)-(max_lat-min_lat))/2.0
        min_lat=min_lat-((max_lon-min_lon)-(max_lat-min_lat))/2.0
    else:
        max_lon=max_lon+((max_lat-min_lat)-(max_lon-min_lon))/2.0
        min_lon=min_lon-((max_lat-min_lat)-(max_lon-min_lon))/2.0

    while(not zl.isConnected()):#check the internet is good or not
        time.sleep(120)   #if no internet, sleep 2 minates try again
    try:
#    print(min_lat,max_lat,max_lon,min_lon)
#    a=1
#    if a==1:
        service = 'Ocean_Basemap'
        xpixels = 5000 
        #Build a map background
        extend=0.1*(max_lon-min_lon)
        map=Basemap(projection='mill',llcrnrlat=min_lat-extend,urcrnrlat=max_lat+extend,llcrnrlon=min_lon-extend,urcrnrlon=max_lon+extend,\
                resolution='f',lat_0=(max_lat+min_lat)/2.0,lon_0=(max_lon+min_lon)/2.0,epsg = 4269)
        map.arcgisimage(service=service, xpixels = xpixels, verbose= False)
        #set the size of step in parallels and draw meridians
        if max_lat-min_lat>=3:
            step=int((max_lat-min_lat)/5.0*10)/10.0
        elif max_lat-min_lat>=1.0:
            step=0.5
        elif max_lat-min_lat>=0.5:
            step=0.2
        else :
            step=0.1
        
        # draw parallels.
        parallels = np.arange(0.,90.0,step)
        map.drawparallels(parallels,labels=[0,1,0,0],fontsize=10,linewidth=0.0)
        # draw meridians
        meridians = np.arange(180.,360.,step)
        map.drawmeridians(meridians,labels=[0,0,0,1],fontsize=10,linewidth=0.0)
        #Draw a scatter plot
        tele_lat,tele_lon=to_list(df['lat'],df['lon'])
        tele_x,tele_y=map(tele_lon,tele_lat)
        ax.plot(tele_x,tele_y,'b*',markersize=6,alpha=0.5,label='telemetry')
        ax.legend()
        #if the path of the picture save is not there, creat the folder
        if not os.path.exists(path_picture_save+'/picture'+dtime.strftime('%Y-%m-%d')+'/'):
            os.makedirs(path_picture_save+'/picture'+dtime.strftime('%Y-%m-%d')+'/')
        #save the map
        plt.savefig(path_picture_save+'/picture'+dtime.strftime('%Y-%m-%d')+'/'+name+'_map'+'_'+dtime.strftime('%Y%m')+'.ps',dpi=dpi) #save picture
        print(name+' finished draw!')
    except KeyboardInterrupt:
        sys.exit()
    except:
        print(name+' need redraw!')


def to_list(lat,lon):
    "transfer the format to list"
    x,y=[],[]
    for i in range(len(lat)):
        x.append(lat[i])
        y.append(lon[i])
    return x,y


path='/home/jmanning/leizhao/programe/aqmain/dictionary/dictionary.json'  # the path of dictionary.json, this file come from the create_modules_dictionary.py
picture_save='/home/jmanning/Desktop/qwe3/' #the directory of dtore picture
end_time=datetime.now()

with open(path,'r') as fp:
    dict=json.load(fp)
#with open(dictionary_path, 'rb') as fp:
#    dict= pickle.load(fp)
    
for i in dict.keys(): #
    if i=='end_time':
        continue
    else: 
        draw_time_series_plot(dict,name=i,dtime=end_time,path_picture_save=picture_save,dpi=300)   # time series plot (semi-annual), about Doppio, FVCOM, GoMOFs.
#        draw_map(dict,name=i,dtime=end_time,path_picture_save=picture_save,dpi=300)  # draw map, the location of observation.

