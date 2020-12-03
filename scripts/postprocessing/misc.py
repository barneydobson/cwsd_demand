# -*- coding: utf-8 -*-
import numpy as np
import matplotlib
from matplotlib import pyplot as plt
import matplotlib as mpl
from numpy.ma import masked_array as ma
import pandas as pd


def shiftedColorMap(cmap, start=0, midpoint=0.5, stop=1.0, name='shiftedcmap'):
    """
    Created on Dec 11 '13 at 19:20
    
    @author: Paul H
    Source: https://stackoverflow.com/questions/7404116/defining-the-midpoint-of-a-colormap-in-matplotlib
    """

    '''
    Function to offset the "center" of a colormap. Useful for
    data with a negative min and positive max and you want the
    middle of the colormap's dynamic range to be at zero.

    Input
    -----
      cmap : The matplotlib colormap to be altered
      start : Offset from lowest point in the colormap's range.
          Defaults to 0.0 (no lower offset). Should be between
          0.0 and `midpoint`.
      midpoint : The new center of the colormap. Defaults to 
          0.5 (no shift). Should be between 0.0 and 1.0. In
          general, this should be  1 - vmax / (vmax + abs(vmin))
          For example if your data range from -15.0 to +5.0 and
          you want the center of the colormap at 0.0, `midpoint`
          should be set to  1 - 5/(5 + 15)) or 0.75
      stop : Offset from highest point in the colormap's range.
          Defaults to 1.0 (no upper offset). Should be between
          `midpoint` and 1.0.
    '''
    cdict = {
        'red': [],
        'green': [],
        'blue': [],
        'alpha': []
    }

    # regular index to compute the colors
    reg_index = np.linspace(start, stop, 257)

    # shifted index to match the data
    shift_index = np.hstack([
        np.linspace(0.0, midpoint, 128, endpoint=False), 
        np.linspace(midpoint, 1.0, 129, endpoint=True)
    ])

    for ri, si in zip(reg_index, shift_index):
        r, g, b, a = cmap(ri)

        cdict['red'].append((si, r, r))
        cdict['green'].append((si, g, g))
        cdict['blue'].append((si, b, b))
        cdict['alpha'].append((si, a, a))

    newcmap = matplotlib.colors.LinearSegmentedColormap(name, cdict)
    plt.register_cmap(cmap=newcmap)

    return newcmap

def unique(sequence):
    seen = set()
    return [x for x in sequence if not (x in seen or seen.add(x))]

def colorgrid_plot(means, isVal = None):
    means = means.copy()
    import copy
    cmap =  copy.copy(mpl.cm.RdYlBu)
    
    #Move flow to top
    if not isVal:
        means = pd.concat([means.loc[['flow']], means.loc[means.index.drop('flow')]])
        means -= 1 #set 0 to neutral
    
    #Separate columns
    cols = means.columns
    if not isVal:
        arcs = list(list(zip(*cols))[0])
        scenarios = list(list(zip(*cols))[1])
        n_scenarios = int(len(set(scenarios)))
    else:
        arcs = means.columns
        scenarios = []
        n_scenarios = 0
    
    n_arcs = int(len(set(arcs)))
    #Create grid spacing
    if not isVal:
        pad_col_grid_after = list(range(n_scenarios, len(cols), n_scenarios))
        pad_row_grid_after = [1]
    else:
        pad_col_grid_after = [8]
        pad_row_grid_after = []
    
    
    pad_size = 0.2
    normal_size = 1
    
    def pad_spacing(no, pad_size, normal_size, gaps):
        count = 0
        spacing = [0]
        for x in range(1,no+1):
            count += normal_size
            spacing.append(count)
            if x in gaps:
                count += pad_size
                spacing.append(count)
        return spacing
    
    x_spacing = pad_spacing(len(means.columns), pad_size, normal_size, pad_col_grid_after)
    if not isVal:
        xt_spacing = [x_spacing[i] + normal_size/2 for i in range(1,len(cols)+n_arcs,n_scenarios+1)]
    else:
        xt_spacing = [x_spacing[i] + normal_size/2 for i in range(1,len(x_spacing),1)]
    y_spacing = pad_spacing(len(means.index), pad_size, normal_size, pad_row_grid_after)
    x_spacing_mgrid,y_spacing_mgrid = np.meshgrid(x_spacing,y_spacing)
    
    #insert dummy columns/rows
    l=0
    for x in pad_col_grid_after:
        means.insert(x+l,'dummy-' + str(l),np.NaN)
        l+=1
    none_row = pd.DataFrame(means.iloc[0,:].copy()).T
    none_row[:] = np.NaN
    none_row.index = ['dummy-row']
    l=0
    for x in pad_row_grid_after:
        means = pd.concat([means.iloc[0:(x+l)],none_row,means.iloc[(x+l):]],axis=0,sort = False)
        l+=1
    
    #Create figure spacing
    f2 = plt.figure(figsize=(10,6))
    if not isVal:
        grid = plt.GridSpec(len(means.index), len(means.columns) + 1, figure = f2)
    else:
        grid = plt.GridSpec(len(means.index), len(means.columns), figure = f2)
    axs2=[]
    axs2.append(plt.subplot(grid[0:len(means.index),0:(len(means.columns))]))
    
    l = 0
    
    if False:
    
        #Iterate over variables of interest
        for idx, row in means.iterrows():
            #Create a masked array of the variable of interest
            bool_mask = np.array(np.ones(means.shape),dtype=bool)
            bool_mask[l] = np.array(np.zeros(means.shape[1]),dtype=bool)
            bool_mask[means.isna()] = False
            means_masked = ma(means.values,bool_mask)
            
            #Shift the colormap to set the neutral colour to 0
            shifted_cmap = shiftedColorMap(cmap,
                                                start=(1-0.5-abs(min(means.values[l]))/max(abs(max(means.values[l])),abs(min(means.values[l])))/2),
                                                stop=(0.5+abs(max(means.values[l]))/max(abs(max(means.values[l])),abs(min(means.values[l])))/2),
                                                name='shifted')
            #Plot color grid
            pm = axs2[0].pcolormesh(x_spacing_mgrid,y_spacing_mgrid,means_masked,linewidth=4,edgecolors='w',cmap=shifted_cmap)
            
        
            
            if idx != 'dummy-row':
            #Create axis for colorbar
                axs2.append(plt.subplot(grid[len(means.index) - l - 4,len(means.columns)]))
                
                #Create colorbar and set axis invisible
                if not isVal:
                    cb = plt.colorbar(pm,ax=axs2[-1],aspect=10,pad=0,orientation="horizontal")
                else:
                    cb = plt.colorbar(pm,ax=axs2[-1],aspect=1,pad=0,orientation="horizontal")
                axs2[-1].set_axis_off()
            
            l+=1
    else:
        bool_mask = np.array(np.ones(means.shape),dtype=bool)
        bool_mask[means.isna()] = False
        means_masked = ma(means.values,means.isna())
        nv = max(abs(means.min().min()), abs(means.max().max()))
        norm = mpl.colors.Normalize(-nv,nv)
        cmap.set_bad(color='white')
        pm = axs2[0].pcolormesh(x_spacing_mgrid,y_spacing_mgrid,means_masked,linewidth=4,edgecolors='w',cmap=cmap,norm=norm)
        # for (i, j), z in np.ndenumerate(means_masked):
        #     axs2[0].text(j, i, '{:0.1f}'.format(z), ha='center', va='center')
        
        
        if not isVal:
            cbar = plt.colorbar(pm)
            cbar.set_label('Relative increase', rotation=270)
        else:
            cbaxes = f2.add_axes([0.1, 0.9, 0.8, 0.03]) 
            cb = plt.colorbar(pm, cbaxes, orientation='horizontal')  
            cb.set_label('Percent Bias')
    #Set ticks and labels
    x_spacing = np.array(x_spacing) + 0.5
    for x in pad_col_grid_after:
        x_spacing = np.delete(x_spacing,x)
        
    y_spacing = np.array(y_spacing) + 0.5
    for y in pad_row_grid_after:
        y_spacing = np.delete(y_spacing,y)
    
    if not isVal:
        axT = axs2[0].secondary_xaxis('top')
        axT.set_xticks(x_spacing[0:-1])
        if 'lockdown' in scenarios:
            axT.set_xticklabels(labels = list(map({'lockdown' : 'LD', 'workapp' : 'AP', 'workfix' :'WH'  ,'popdec' : 'PD'}.get, scenarios)))
        else:
            axT.set_xticklabels(scenarios)
        axs2[0].set_xticks(xt_spacing)
        axs2[0].set_yticks(y_spacing[0:-1])
        axs2[0].set_xticklabels(labels=unique(arcs),rotation=45)
        axs2[0].set_yticklabels(labels=means.dropna(axis=0,how='all').index)
        axs2[0].set_aspect(1) #If you want squares (but can screw with the colorbars)
    else:
        axs2[0].set_xticks(x_spacing[0:-1])
        axs2[0].set_yticks(y_spacing[0:-1])
        axs2[0].set_xticklabels(labels=arcs,rotation=45)
        axs2[0].set_yticklabels(labels=means.dropna(axis=0,how='all').index)
        axs2[0].set_aspect(1) #If you want squares (but can screw with the colorbars)
    return f2

