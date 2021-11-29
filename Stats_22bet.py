import concurrent.futures
import requests
import time
import csv
import os
import difflib
import pickle
from unidecode import unidecode

def load_url(url, timeout):
    ans = requests.get(url, timeout=timeout)
    return ans

def calc_stats(stats):
    [attacks_h, attacks_a, dang_attacks_h, dang_attacks_a, shotson_h, shotson_a, shotsoff_h, shotsoff_a]=stats
    if attacks_h+attacks_a+dang_attacks_h+dang_attacks_a>50:
        shot_weight=8
    else:
        shot_weight=6
    total_h = attacks_h + dang_attacks_h * 3 + shotson_h * shot_weight + shotsoff_h * shot_weight
    total_a = attacks_a + dang_attacks_a * 3 + shotson_a * shot_weight + shotsoff_a * shot_weight
    if total_h+total_a<30:
        stats = -100
    else:
        stats = 100 * (total_h) / (total_h + total_a)

    return stats

def real_game(data):
    if '3x3' not in i['O1'] and \
        '3'+chr(1093)+'3' not in i['O1'] and \
        '4x4' not in i['O1'] and \
        '4'+chr(1093)+'4' not in i['O1'] and \
        '5x5' not in i['O1'] and \
        '5'+chr(1093)+'5' not in i['O1'] and \
        '3x3' not in i['L'] and \
        '3'+chr(1093)+'3' not in i['L'] and \
        '4x4' not in i['L'] and \
        '4'+chr(1093)+'4' not in i['L'] and \
        '5x5' not in i['L'] and \
        '5'+chr(1093)+'5' not in i['L'] and \
        '6x6' not in i['L'] and \
        '6'+chr(1093)+'6' not in i['L'] and \
        'Cyber ' not in i['L'] and \
        'Esports' not in i['L'] and \
        'Soap Soccer' not in i['L'] and \
        'Table Soccer' not in i['L'] and \
        'TS' in i['SC'] and \
        ('V' not in i or (i['V']!='Extra-Time' and i['V']!='Penalty Shoot-out')) and \
        'ST' in i['SC']:
        return True
    else:
        return False


start=time.time()
curr_path=os.path.dirname(os.path.abspath(__file__))
filename=curr_path+'\\prev.pickle'
if os.path.exists(filename):
    with open(filename, 'rb') as f:
        [prev,prev_time] = pickle.load(f)
else:
    prev=[]
    prev_time=[]

#live_events=requests.get('https://22bet.com/LiveFeed/Get1x2_VZip?count=5000&lng=en&antisports=2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65,66,67,68,69,70,71,72,73,74,75,76,77,78,79,80,81,83,84,85,86,87,88,89,90,91,92,94,95,96,97,98,99,100,101,102,103,105,106,107,109,110,111,112,113,114,115,116,117,118,119,120,121,122,125,126,128,129,130,131,132,133,135,136,137,138,139,140,141,142,143,144,145,146,147,148,149,150,151,152,153,154,155,156,157,158,159,160,161,162,164,165,166,168,169,170,171,172,173,174,175,176,177,178,179,180,181,182,183,184,187,188,189,190,191,192,193,194,195,196,197,198,199,200,201,202,203,204,205,206,207,208,209,210,211,212,213,214,215,216,217,218,219,220,221,222,223,224,225,226,227,228,229,230,231,232,233,235,237,238,239,240,241,242,243,244,245,246,247,248,249,250,251,252,253,254,255,256,257,258,259,260,261,262,263,264,265,266,267&mode=4&partner=151').json()['Value']
live_events=requests.get('https://22bets.me/LiveFeed/Get1x2_VZip?count=5000&lng=en&antisports=2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65,66,67,68,69,70,71,72,73,74,75,76,77,78,79,80,81,83,84,85,86,87,88,89,90,91,92,94,95,96,97,98,99,100,101,102,103,105,106,107,109,110,111,112,113,114,115,116,117,118,119,120,121,122,125,126,128,129,130,131,132,133,135,136,137,138,139,140,141,142,143,144,145,146,147,148,149,150,151,152,153,154,155,156,157,158,159,160,161,162,164,165,166,168,169,170,171,172,173,174,175,176,177,178,179,180,181,182,183,184,187,188,189,190,191,192,193,194,195,196,197,198,199,200,201,202,203,204,205,206,207,208,209,210,211,212,213,214,215,216,217,218,219,220,221,222,223,224,225,226,227,228,229,230,231,232,233,235,237,238,239,240,241,242,243,244,245,246,247,248,249,250,251,252,253,254,255,256,257,258,259,260,261,262,263,264,265,266,267&mode=4&partner=151').json()['Value']
football_events=[]
urls=[]
for i in live_events:
    if real_game(i):
        if i['SC']['TS']>0:
            football_events.append(i['O1'] + ' - ' + i['O2'])
            urls.append('https://22bets.me/LiveFeed/GetGameZip?id='+str(i['I'])+'&lng=en&tzo=1&cfview=0&isSubGames=true&GroupEvents=true&countevents=4000&partner=151')
            #urls.append('https://22bet.com/LiveFeed/GetGameZip?id='+str(i['I'])+'&lng=en&tzo=1&cfview=0&isSubGames=true&GroupEvents=true&countevents=4000&partner=151')

CONNECTIONS = len(urls)
CONNECTIONS = 50
#CONNECTIONS = round(len(urls)/2)
TIMEOUT = 10
live_events_detailed=[]

# start_time=time.time()
# for url in urls:
#     a=requests.get(url, timeout=5)
# print("WEIRD: " + str(time.time()-start_time))

with concurrent.futures.ThreadPoolExecutor(max_workers=CONNECTIONS) as executor:
    future_to_url = (executor.submit(load_url, url, TIMEOUT) for url in urls)
    for future in concurrent.futures.as_completed(future_to_url):
        try:
            #if start_time=0:
            #    start_time
            data = future.result().json()['Value']
            id=data['I']
            minutes = round(data['SC']['TS']/60)
            competition = data['L']
            team = data['O1']
            game = data['O1'] + ' - ' + data['O2']
            score_home = data['SC']['FS']['S1'] if 'S1' in data['SC']['FS'] else 0
            score_away = data['SC']['FS']['S2'] if 'S2' in data['SC']['FS'] else 0
            attacks_h=sum([int(stat['Value'] or 0) if stat['Key']=='Attacks1' else 0 for stat in data['SC']['S']])
            attacks_a=sum([int(stat['Value'] or 0) if stat['Key']=='Attacks2' else 0 for stat in data['SC']['S']])
            dang_attacks_h=sum([int(stat['Value'] or 0) if stat['Key']=='DanAttacks1' else 0 for stat in data['SC']['S']])
            dang_attacks_a=sum([int(stat['Value'] or 0) if stat['Key']=='DanAttacks2' else 0 for stat in data['SC']['S']])
            shotson_h=sum([int(stat['Value'] or 0) if stat['Key']=='ShotsOn1' else 0 for stat in data['SC']['S']])
            shotson_a=sum([int(stat['Value'] or 0) if stat['Key']=='ShotsOn2' else 0 for stat in data['SC']['S']])
            shotsoff_h=sum([int(stat['Value'] or 0) if stat['Key']=='ShotsOff1' else 0 for stat in data['SC']['S']])
            shotsoff_a=sum([int(stat['Value'] or 0) if stat['Key']=='ShotsOff2' else 0 for stat in data['SC']['S']])
            possession_h=sum([int(stat['S1']) if stat['ID']==29 else 0 for stat in data['SC']['ST'][0]['Value']])
            possession_a=sum([int(stat['S2']) if stat['ID']==29 else 0 for stat in data['SC']['ST'][0]['Value']])
            reds_h=sum([int(stat['Value'] or 0) if stat['Key']=='IRedCard1' else 0 for stat in data['SC']['S']])
            reds_a=sum([int(stat['Value'] or 0) if stat['Key']=='IRedCard2' else 0 for stat in data['SC']['S']])
            statsp = calc_stats([attacks_h,attacks_a,dang_attacks_h,dang_attacks_a,shotson_h,shotson_a,shotsoff_h,shotsoff_a])
            found_g=False
            found_h=False
            found_gh=False
            if minutes<43:
                if 'SG' in data:
                    if 'GE' in data['SG'][0]:
                        for betoffer in data['SG'][0]['GE']:
                            if betoffer['G'] == 2:  # handicap
                                index = -1
                                for find_index in range(len(betoffer['E'][0])):
                                    if 'P' in betoffer['E'][0][find_index]:
                                        handicap = betoffer['E'][0][find_index]['P']
                                    else:
                                        handicap = 0
                                    #if 'CE' in betoffer['E'][0][find_index] and ('P' not in betoffer['E'][0][find_index] or betoffer['E'][0][find_index]['P']==score_away-score_home):
                                    if handicap == score_away - score_home:
                                        index = find_index
                                if index >= 0:
                                    odd_home_h = betoffer['E'][0][index]['C']
                                    odd_away_h = betoffer['E'][1][index]['C']
                                    oddsp_h = 100 * (1 / odd_home_h) / (1 / odd_home_h + 1 / odd_away_h)
                                    found_h = True

            for betoffer in data['GE']:
                if betoffer['G']==20 and len(betoffer['E'])==3 and (betoffer['E'][0][0]['C']!= 1.001 or betoffer['E'][1][0]['C']!= 1.001): #next goal
                    odd_home_g = betoffer['E'][0][0]['C']
                    odd_away_g = betoffer['E'][1][0]['C']
                    oddsp_g = 100 * (1 / odd_home_g) / (1 / odd_home_g + 1 / odd_away_g)
                    found_g=True
                if betoffer['G']==2 and minutes>=45: #handicap
                    index=-1
                    for find_index in range(len(betoffer['E'][0])):
                        if 'P' in betoffer['E'][0][find_index]:
                            handicap=betoffer['E'][0][find_index]['P']
                        else:
                            handicap=0
                        #if 'CE' in betoffer['E'][0][find_index] and ('P' not in betoffer['E'][0][find_index] or betoffer['E'][0][find_index]['P']==score_away-score_home):
                        if handicap==score_away-score_home:
                            index=find_index
                    if index>=0:
                        odd_home_h = betoffer['E'][0][index]['C']
                        odd_away_h = betoffer['E'][1][index]['C']
                        oddsp_h = 100 * (1 / odd_home_h) / (1 / odd_home_h + 1 / odd_away_h)
                        found_h = True
                if betoffer['G'] == 3559:  # next goal with handicap
                    odd_home_gh = betoffer['E'][0][0]['C']
                    odd_away_gh = betoffer['E'][1][0]['C']
                    oddsp_gh = 100 * (1 / odd_home_gh) / (1 / odd_home_gh + 1 / odd_away_gh)
                    found_gh = True
            if found_gh or found_h or found_g:
                odd_type=''
                if found_gh and found_h and (minutes>=65 or (minutes>=25 and minutes<45)) :
                    if minutes>=65 and abs(oddsp_gh - statsp) > abs(oddsp_h - statsp):
                        odd_home = odd_home_gh
                        odd_away = odd_away_gh
                        oddsp = oddsp_gh
                        odd_type='Next Goal, Handicap'
                    elif (minutes>=25 and minutes<45) and abs(oddsp_gh - statsp) > abs(oddsp_h - statsp)+5:
                        odd_home = odd_home_gh
                        odd_away = odd_away_gh
                        oddsp = oddsp_gh
                        odd_type = 'Next Goal, Handicap'
                    else:
                        odd_home = odd_home_h
                        odd_away = odd_away_h
                        oddsp = oddsp_h
                        if minutes<45:
                            odd_type='1st half Handicap'
                        else:
                            odd_type='Final Handicap'
                elif found_gh:
                    odd_home = odd_home_gh
                    odd_away = odd_away_gh
                    oddsp = oddsp_gh
                    odd_type = 'Next Goal, Handicap'
                elif found_h:
                    if (minutes>=65 or (minutes>=25 and minutes<45)) or not(found_g):
                        odd_home = odd_home_h
                        odd_away = odd_away_h
                        oddsp = oddsp_h
                        if minutes<45:
                            odd_type='1st half Handicap'
                        else:
                            odd_type='Final Handicap'
                    else:
                        odd_home = odd_home_g
                        odd_away = odd_away_g
                        oddsp = oddsp_g
                        odd_type = 'Next Goal'
                else:
                    odd_home = odd_home_g
                    odd_away = odd_away_g
                    oddsp = oddsp_g
                    odd_type = 'Next Goal'

                if statsp == -100:
                    live_events_detailed.append([minutes, competition, game, oddsp, 0, '',odd_home, odd_away,attacks_h,attacks_a,dang_attacks_h,dang_attacks_a,shotson_h,shotson_a,shotsoff_h,shotsoff_a, possession_h, possession_a,score_home, score_away, reds_h, reds_a,odd_type])
                    #live_events_detailed.append([minutes, competition, game, oddsp, 0, '',odd_home, odd_away,attacks_h,attacks_a,dang_attacks_h,dang_attacks_a,shotson_h,shotson_a,shotsoff_h,shotsoff_a, possession_h, possession_a,score_home, score_away])
                else:
                    live_events_detailed.append([minutes, competition, game, oddsp, statsp, oddsp - statsp, odd_home, odd_away, attacks_h,attacks_a,dang_attacks_h,dang_attacks_a,shotson_h,shotson_a,shotsoff_h,shotsoff_a, possession_h, possession_a,score_home, score_away, reds_h, reds_a,odd_type])
                    #live_events_detailed.append([minutes, competition, game, oddsp, statsp, oddsp - statsp, odd_home, odd_away, attacks_h,attacks_a,dang_attacks_h,dang_attacks_a,shotson_h,shotson_a,shotsoff_h,shotsoff_a, possession_h, possession_a,score_home, score_away])
            else:
                if minutes<89:
                    print("No bet for game in " + str(minutes) + "min: " + game)
        except Exception as exc:
            data = str(type(exc))
            print("THERE WAS AN ERROR")

i=0
while i<len(prev):
    if prev[i][0] not in football_events and (time.time()-prev_time[0])/3600>2.5:
        prev[i]=0
        prev_time[i]=0
    i+=1

prev=[i for i in prev if i!=0]
prev_time=[i for i in prev_time if i!=0]

for i in live_events_detailed:
    if i[2] in [j[0] for j in prev]:
        prev_index=[j[0] for j in prev].index(i[2])
        index_live=live_events_detailed.index(i)
        live_events_detailed[index_live] = i + prev[prev_index][1:]
        prev[prev_index][27:39]=prev[prev_index][14:26]
        prev[prev_index][14:26]=prev[prev_index][1:13]
        prev[prev_index][1:13]=[i[0]]+[i[4]]+i[8:18]
        diff_stats1=calc_stats([x1 - x2 for (x1, x2) in zip(live_events_detailed[index_live][8:16], live_events_detailed[index_live][25:33])])
        diff_stats2=calc_stats([x1 - x2 for (x1, x2) in zip(live_events_detailed[index_live][8:16], live_events_detailed[index_live][38:46])])
        diff_stats3=calc_stats([x1 - x2 for (x1, x2) in zip(live_events_detailed[index_live][8:16], live_events_detailed[index_live][51:59])])

        if diff_stats1 == -100 or live_events_detailed[index_live][23]==0:
            live_events_detailed[index_live][35]=''
        else:
            live_events_detailed[index_live][35]=i[3]-diff_stats1

        if diff_stats2 == -100 or live_events_detailed[index_live][36]==0:
            live_events_detailed[index_live][48]=''
        else:
            live_events_detailed[index_live][48]=i[3]-diff_stats2

        if diff_stats3 == -100 or live_events_detailed[index_live][49]==0:
            live_events_detailed[index_live][61]=''
        else:
            live_events_detailed[index_live][61]=i[3]-diff_stats3
    else:
        prev.append([i[2]]+[i[0]]+[i[4]]+i[8:18]+[i[3]-i[4]]+[0]*26)
        prev_time.append(time.time())

print("Number of games: " + str(len(live_events_detailed)))

with open(filename, 'wb') as f:
    pickle.dump([prev,prev_time], f)

header=["Minutes","Competition","Game","Odds%","Stats%","Diff%"]
filename=curr_path+'\\Initial.txt'
with open(filename, 'w', newline='', encoding='utf-8') as f:
    movies = csv.writer(f,delimiter=';')
    movies.writerow(header)
    for x in live_events_detailed:
        movies.writerow(x)

end=time.time()
print(end-start)
filename=curr_path+'\\done.txt'
with open(filename, 'w', newline='') as f:
    f.write("done")
