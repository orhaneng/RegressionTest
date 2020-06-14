import os

os.system("source activate base")
import pandas as pd
import shutil

from multiprocessing import Pool
import tqdm

title = "GEOTAB"
regressionanalysislink = "/Users/omerorhan/Documents/chris/trip_results3.4.3geotab10k.csv"


#title = "NON-GEOTAB"
#regressionanalysislink = "/Users/omerorhan/Documents/chris/trip_results3.4.3nongeotab10k.csv"


#title = "NON-GEOTAB (1K)"
#regressionanalysislink = "/Users/omerorhan/Documents/chris/trip_results3.4.3nongeotab1K.csv"


def regressiondataanalysis():
    data = pd.read_csv(
        regressionanalysislink)

    dictscore = {"100-499 RISKY": 0, "500-599 POOR": 0, "560-709 AVERAGE": 0, "710-799 GOOD": 0, "800-850 GREAT": 0}

    for item in dictscore.keys():
        dictscore[item] = ((len(data[(data['score'] >= int(item.split("-")[0])) & (
                data['score'] <= int(item.split("-")[1].split(' ')[0]))])) * 100) / len(data)

        '''    
        if item != "None" and int(item) == 850:
            count = dictscore["850"]
            count = count + 1
            dictscore["850"] = count
        '''
    '''
    from matplotlib import pyplot as plt
    import numpy as np
    fig = plt.figure()
    ax = fig.add_axes([0, 0, 1, 1])
    ax.axis('equal')
    ax.pie(dictscore.values(), labels=dictscore.keys(), autopct='%1.2f%%')
    # ax.pie([20,30,60], labels=["a","b","c"], autopct='%1.2f%%')
    plt.show()
    print(dictscore)

    totalcount = len(data)
    for key, value in dictscore.items():
        dictscore[key] = round((value * 100) / totalcount)
    dictscore = dictscore
    '''

    import matplotlib.pyplot as plt
    # The slices will be ordered and plotted counter-clockwise.

    patches, texts = plt.pie(dictscore.values(), startangle=90)
    plt.legend(patches, dictscore.keys(), loc="best",
               labels=['%s, %1.1f %%' % (l, s) for l, s in zip(dictscore.keys(), dictscore.values())])
    # Set aspect ratio to be equal so that pie is drawn as a circle.
    plt.axis('equal')
    plt.title(title + " FICO SCORE CATEGORIES")
    plt.tight_layout()
    plt.show()
    print(dictscore)

    dictscore = {"100-849": 0, "850-900 GREAT": 0}
    for item in dictscore.keys():
        dictscore[item] = ((len(data[(data['score'] >= int(item.split("-")[0])) & (
                data['score'] <= int(item.split("-")[1].split(' ')[0]))])) * 100) / len(data)
    import matplotlib.pyplot as plt
    # The slices will be ordered and plotted counter-clockwise.
    dictscore.update({"850": dictscore["850-900 GREAT"]})
    del dictscore["850-900 GREAT"]

    patches, texts = plt.pie(dictscore.values(), startangle=90)
    plt.legend(patches, dictscore.keys(), loc="best",
               labels=['%s, %1.1f %%' % (l, s) for l, s in zip(dictscore.keys(), dictscore.values())])
    # Set aspect ratio to be equal so that pie is drawn as a circle.
    plt.axis('equal')
    plt.title(title + " FICO SCORE 850")
    plt.tight_layout()
    plt.show()


#regressiondataanalysis()


def eventAnalysischart(link):
    data = pd.read_csv(link)

    events = ['HARD_ACCELERATION', 'HARD_BRAKING', 'HARD_CORNERING',
              'PHONE_MANIPULATION', 'SPEEDING']

    data = data.rename(columns={"hard_acceleration_count": "HARD_ACCELERATION", "hard_braking_count": "HARD_BRAKING"
        , "hard_cornering_count": "HARD_CORNERING", "phone_manipulation_count": "PHONE_MANIPULATION",
                                "displayed_speeding_count": "SPEEDING"})

    from itertools import combinations

    # ONE COMBINATION
    comb = combinations(events, 1)
    df_result = pd.DataFrame(
        columns=['Combination', 'Event Combinations', '% of trips in only one category'])
    for item in list(comb):
        events = ['HARD_ACCELERATION', 'HARD_BRAKING', 'HARD_CORNERING',
                  'PHONE_MANIPULATION', 'SPEEDING']
        events.remove(item[0])
        count = len(
            data[(data[item[0]] != 0) & (data[events[0]] == 0) & (data[events[1]] == 0) & (
                    data[events[2]] == 0) & (data[events[3]] == 0)])

        percent = round((count * 100) / len(data), 2)
        df_result = df_result.append(
            {'Combination': 1, 'Event Combinations': item[0], '% of trips in only one category': percent},
            ignore_index=True)
    df_result = df_result.sort_values(by='% of trips in only one category', ascending=False)
    return df_result


    '''
    data = pd.read_csv(regressionanalysislink)

    events = {'hard_acceleration_count': 0, 'hard_braking_count': 0, 'hard_cornering_count': 0,
              'phone_manipulation_count': 0, 'displayed_speeding_count': 0}

    # for key, value in events.items():
    #    events[key] = round((len(data[data[key] != 0]) / len(data)) * 100, 2)
    onlyhardaccleration = data[
        (data['hard_acceleration_count'] != 0) & (data['hard_braking_count'] == 0) & (data['hard_cornering_count'] == 0)
        & (data['phone_manipulation_count'] == 0) & (data['displayed_speeding_count'] == 0)]

    onlyhardbraking = data[
        (data['hard_acceleration_count'] == 0) & (data['hard_braking_count'] != 0) & (data['hard_cornering_count'] == 0)
        & (data['phone_manipulation_count'] == 0) & (data['displayed_speeding_count'] == 0)]

    onlyhardcornering = data[
        (data['hard_acceleration_count'] == 0) & (data['hard_braking_count'] == 0) & (data['hard_cornering_count'] != 0)
        & (data['phone_manipulation_count'] == 0) & (data['displayed_speeding_count'] == 0)]

    onlyPhonemanipulation = data[
        (data['hard_acceleration_count'] == 0) & (data['hard_braking_count'] == 0) & (data['hard_cornering_count'] == 0)
        & (data['phone_manipulation_count'] != 0) & (data['displayed_speeding_count'] == 0)]

    onlySpeeding = data[
        (data['hard_acceleration_count'] == 0) & (data['hard_braking_count'] == 0) & (data['hard_cornering_count'] == 0)
        & (data['phone_manipulation_count'] == 0) & (data['displayed_speeding_count'] != 0)]

    df_result = pd.DataFrame(
        columns=['Event Type', '% of trips in only one category'])

    df_result = df_result.append({'Event Type': "HARD_ACCELERATION", '% of trips in only one category': round(
        ((len(onlyhardaccleration) * 100) / len(data)), 2)},
                                 ignore_index=True)

    df_result = df_result.append({'Event Type': "HARD_BRAKING", '% of trips in only one category': round(
        ((len(onlyhardbraking) * 100) / len(data)), 2)},
                                 ignore_index=True)

    df_result = df_result.append({'Event Type': "HARD_CORNERING", '% of trips in only one category': round(
        ((len(onlyhardcornering) * 100) / len(data)), 2)},
                                 ignore_index=True)

    df_result = df_result.append({'Event Type': "PHONE_MANIPULATION", '% of trips in only one category': round(
        ((len(onlyPhonemanipulation) * 100) / len(data)), 2)},
                                 ignore_index=True)

    df_result = df_result.append({'Event Type': "SPEEDING", '% of trips in only one category': round(
        ((len(onlySpeeding) * 100) / len(data)), 2)},
                                 ignore_index=True)

    df_result = df_result.sort_values(by='% of trips in only one category', ascending=False)


    '''
    #df_result.to_csv("/Users/omerorhan/Documents/chris/events.csv")



#eventAnalysischart(link)


def graphevents():
    title = "GEOTAB"
    regressionanalysislink = "/Users/omerorhan/Documents/chris/trip_results3.4.3geotab10k.csv"
    geotab = eventAnalysischart(regressionanalysislink)
    geotab = geotab.drop(columns=['Combination'])
    title = "NON-GEOTAB"
    regressionanalysislink = "/Users/omerorhan/Documents/chris/trip_results3.4.3nongeotab10k.csv"

    nongeotab = eventAnalysischart(regressionanalysislink)
    nongeotab = nongeotab.drop(columns=['Combination'])
    import numpy as np
    import matplotlib.pyplot as plt

    # data to plot
    n_groups = 5
    means_frank = geotab['% of trips in only one category'].to_list()
    means_guido = nongeotab['% of trips in only one category'].to_list()

    # create plot
    fig, ax = plt.subplots()
    index = np.arange(n_groups)
    bar_width = 0.35
    opacity = 0.8

    rects1 = plt.bar(index, means_frank, bar_width,
                     alpha=opacity,

                     label='GEOTAB')

    rects2 = plt.bar(index + bar_width, means_guido, bar_width,
                     alpha=opacity,
                     label='NON-GEOTAB')

    #plt.xlabel('Event')
    plt.ylabel('% of trips')
    plt.title('% of trips in only one category')
    plt.xticks(index + bar_width, geotab['Event Combinations'])
    plt.legend()

    plt.tight_layout()
    plt.show()

graphevents()

def eventAnalysiscombinationchart(regressionanalysislink, tit):
    data = pd.read_csv(regressionanalysislink)

    events = ['HARD_ACCELERATION', 'HARD_BRAKING', 'HARD_CORNERING',
              'PHONE_MANIPULATION', 'SPEEDING']

    data = data.rename(columns={"hard_acceleration_count": "HARD_ACCELERATION", "hard_braking_count": "HARD_BRAKING"
        , "hard_cornering_count": "HARD_CORNERING", "phone_manipulation_count": "PHONE_MANIPULATION",
                                "displayed_speeding_count": "SPEEDING"})

    from itertools import combinations

    # TWO COMBINATION
    comb = combinations(events, 2)
    df_result = pd.DataFrame(
        columns=['Combination', 'Event Combinations', '% of trips' + tit])
    for item in list(comb):
        events = ['HARD_ACCELERATION', 'HARD_BRAKING', 'HARD_CORNERING',
                  'PHONE_MANIPULATION', 'SPEEDING']
        events.remove(item[0])
        events.remove(item[1])
        count = len(
            data[(data[item[0]] != 0) & (data[item[1]] != 0) & (data[events[0]] == 0) & (data[events[1]] == 0) & (
                    data[events[2]] == 0)])

        percent = round((count * 100) / len(data), 2)
        df_result = df_result.append(
            {'Combination': 2, 'Event Combinations': item[1] + " - " + item[0], '% of trips' + tit: percent},
            ignore_index=True)

    # THREE COMBINATION

    events = ['HARD_ACCELERATION', 'HARD_BRAKING', 'HARD_CORNERING',
              'PHONE_MANIPULATION', 'SPEEDING']
    comb = combinations(events, 3)

    for item in list(comb):
        events = ['HARD_ACCELERATION', 'HARD_BRAKING', 'HARD_CORNERING',
                  'PHONE_MANIPULATION', 'SPEEDING']
        events.remove(item[0])
        events.remove(item[1])
        events.remove(item[2])
        count = len(
            data[(data[item[0]] != 0) & (data[item[1]] != 0) & (data[item[2]] != 0) & (data[events[0]] == 0) & (
                    data[events[1]] == 0)])

        percent = round((count * 100) / len(data), 2)
        df_result = df_result.append(
            {'Combination': 3, 'Event Combinations': item[0] + " - " + item[1] + " - " + item[2],
             '% of trips' + tit: percent},
            ignore_index=True)

    # FOUR COMBINATION

    events = ['HARD_ACCELERATION', 'HARD_BRAKING', 'HARD_CORNERING',
              'PHONE_MANIPULATION', 'SPEEDING']
    comb = combinations(events, 4)

    for item in list(comb):
        events = ['HARD_ACCELERATION', 'HARD_BRAKING', 'HARD_CORNERING',
                  'PHONE_MANIPULATION', 'SPEEDING']
        events.remove(item[0])
        events.remove(item[1])
        events.remove(item[2])
        events.remove(item[3])
        count = len(
            data[(data[item[0]] != 0) & (data[item[1]] != 0) & (data[item[2]] != 0) & (data[item[3]] != 0) & (
                    data[events[0]] == 0)])

        percent = round((count * 100) / len(data), 2)
        df_result = df_result.append(
            {'Combination': 4, 'Event Combinations': item[0] + " - " + item[1] + " - " + item[2] + " - " + item[3],
             '% of trips' + tit: percent},
            ignore_index=True)

    # FIVE COMBINATION

    events = ['HARD_ACCELERATION', 'HARD_BRAKING', 'HARD_CORNERING',
              'PHONE_MANIPULATION', 'SPEEDING']

    count = len(
        data[(data[events[0]] != 0) & (data[events[1]] != 0) & (data[events[2]] != 0) & (data[events[3]] != 0) & (
                data[events[4]] != 0)])

    percent = round((count * 100) / len(data), 2)
    df_result = df_result.append(
        {'Combination': 5,
         'Event Combinations': events[0] + " - " + events[1] + " - " + events[2] + " - " + events[3] + " - " + events[
             4],
         '% of trips' + tit: percent},
        ignore_index=True)
    df_result = df_result.sort_values(by=['Event Combinations'], ascending=False)

    return df_result

def sss():
    title = "GEOTAB (10K)"
    regressionanalysislink = "/Users/omerorhan/Documents/chris/trip_results3.4.3geotab10k.csv"

    res1 = eventAnalysiscombinationchart(regressionanalysislink, title)

    title = "NON-GEOTAB (10K)"
    regressionanalysislink = "/Users/omerorhan/Documents/chris/trip_results3.4.3nongeotab10k.csv"

    res2 = eventAnalysiscombinationchart(regressionanalysislink, title)

    title = "NON-GEOTAB (1K)"
    regressionanalysislink = "/Users/omerorhan/Documents/chris/trip_results3.4.3nongeotab1K.csv"
    res3 = eventAnalysiscombinationchart(regressionanalysislink, title)

    res1 = res1.drop(columns=['Combination'])
    res2 = res2.drop(columns=['Combination'])
    res3 = res3.drop(columns=['Combination'])

    result = pd.concat([res1, res2,res3], ignore_index=True, keys='Event Combinations')

    result.to_csv("/Users/omerorhan/Documents/chris/events.csv")


def distanceAnalysis():
    data = pd.read_csv(regressionanalysislink)

    data['distance_mile'] = round(data['distance'] * 0.000621371)

    dict = {"0-25 MILES": 0, "25-50 MILES": 0, "50-75 MILES": 0, "75-100 MILES": 0, "101-150 MILES": 0,
            "151-200 MILES": 0, "201-250 MILES": 0,
            "251-300 MILES": 0}

    for item in dict.keys():
        dict[item] = ((len(data[(data['distance_mile'] >= int(item.split("-")[0])) & (
                data['distance_mile'] <= int(item.split("-")[1].split(' ')[0]))])) * 100) / len(data)

    '''
    from matplotlib import pyplot as plt
    import numpy as np
    fig = plt.figure()
    ax = fig.add_axes([0, 0, 1, 1])
    ax.axis('equal')
    explode = (0, 0, 0, 0,0,0,0.1)

    ax.pie(dict.values(),explode=explode, labels=dict.keys(), autopct='%1.2f%%')
    # ax.pie([20,30,60], labels=["a","b","c"], autopct='%1.2f%%')
    plt.legend(patches, labels, loc="best")
    plt.show()
    '''
    import matplotlib.pyplot as plt
    # The slices will be ordered and plotted counter-clockwise.

    patches, texts = plt.pie(dict.values(), startangle=90)
    plt.legend(patches, dict.keys(), loc="best",
               labels=['%s, %1.1f %%' % (l, s) for l, s in zip(dict.keys(), dict.values())])
    # Set aspect ratio to be equal so that pie is drawn as a circle.
    plt.axis('equal')
    plt.title(title + " - Distance")

    plt.tight_layout()
    plt.show()
    print(dictscore)


#distanceAnalysis()

def durationAnalysis():
    data = pd.read_csv(regressionanalysislink)

    data['duration_hour'] = (round(data['duration']) * 0.000277778)

    dict = {"0-1 HOUR": 0, "1-2 HOURS": 0, "2-3 HOURS": 0, "3-4 HOURS": 0, "4-5 HOURS": 0, "5-6 HOURS": 0,
            "6-7 HOURS": 0, "7-8 HOURS": 0, "8-9 HOURS": 0, "9-10 HOURS": 0, "10-11 HOURS": 0, "11-12 HOURS": 0}
    count = (len(data[data['duration_hour'] > 12]))

    counttotal = len(data)

    counttotal = counttotal- count
    for item in dict.keys():
        dict[item] = ((len(data[(data['duration_hour'] >= int(item.split("-")[0])) & (
                data['duration_hour'] < int(item.split("-")[1].split(' ')[0]))])) * 100) / counttotal

    '''
    from matplotlib import pyplot as plt
    import numpy as np
    fig = plt.figure()
    ax = fig.add_axes([0, 0, 1, 1])
    ax.axis('equal')
    explode = (0, 0, 0, 0,0,0,0.1)

    ax.pie(dict.values(),explode=explode, labels=dict.keys(), autopct='%1.2f%%')
    # ax.pie([20,30,60], labels=["a","b","c"], autopct='%1.2f%%')
    plt.legend(patches, labels, loc="best")
    plt.show()
    '''
    import matplotlib.pyplot as plt
    from matplotlib import cm
    import matplotlib.colors as mcolors
    import numpy as np
    import random
    colors = random.choices(list(mcolors.CSS4_COLORS.values()), k=14)
    # The slices will be ordered and plotted counter-clockwise.
    a = np.random.random(40)
    cs = cm.Set1(np.arange(40) / 40.)
    patches, texts = plt.pie(dict.values(), startangle=90)
    plt.legend(patches, dict.keys(), loc="best",
               labels=['%s, %1.1f %%' % (l, s) for l, s in zip(dict.keys(), dict.values())])
    # Set aspect ratio to be equal so that pie is drawn as a circle.
    plt.axis('equal')
    plt.title(title + " - Duration")
    plt.tight_layout()
    plt.show()

#durationAnalysis()
