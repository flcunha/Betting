import concurrent.futures
import requests
import time
import csv
import os
import difflib
import pickle
from unidecode import unidecode
import math
from urllib.request import urlopen
from json import load

def load_url(url, timeout):
    ans = requests.get(url, headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.63 Safari/537.36"}, timeout=timeout)
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

def real_game(i):
    if i['sport_id'] == 2 and i['is_live'] and str(data['id']) in live_events['results']:
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

url = 'https://ipinfo.io/json'
res = urlopen(url)
data = load(res)
if data['country']=='LT':
    print('LT')
    live_events=requests.get('https://nodejs.tglab.io/cache/5/en/lt/live-events.json?hidenseek=9f3bd228f35e7f8d9c99eb3916eee9ea6d8c8172', headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.63 Safari/537.36"}).json()
else:
    print('Not LT')
    live_events=requests.get('https://nodejs.tglab.io/cache/5/en/cz/live-events.json?hidenseek=a03984ce56390b61e9f38a8ee7f512d5b5656e22', headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.63 Safari/537.36"}).json()


#'nodejs.tglab.io/cache/5/1/season/67639/h2h/7050/4741/match/27976800'
#This gives the odds: requests.get('https://nodejs.tglab.io/cache/5/en/cz/4013506/in-play-odds-by-event.json?hidenseek=a03912184ce56390b61e9f38a8ee7f512d5b5656e224', headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.63 Safari/537.36"}).json()
#This gives the statistics:

football_events=[]
urls=[]
live_events_aux={}
for i in live_events['events']:
    data=live_events['events'][i]
    if real_game(data):
        #competition=data['tournament_name']
        competition=data['country_name']['en'] + ' - ' + data['tournament_name']['en']
        live_events_aux[i]=competition
        football_events.append(data['teams']['home'] + ' - ' + data['teams']['away'])
        urls.append('https://stats.tsports.online/cache/5/en/event/' + str(i) + '.json')

CONNECTIONS = len(urls)
CONNECTIONS = 50
#CONNECTIONS = round(len(urls)/2)
TIMEOUT = 10
live_events_detailed=[]

urls_odds={}
info_games={}

with concurrent.futures.ThreadPoolExecutor(max_workers=CONNECTIONS) as executor:
    future_to_url = (executor.submit(load_url, url, TIMEOUT) for url in urls)
    for future in concurrent.futures.as_completed(future_to_url):
        #try:
        if 1==1:
            if future.result().status_code!=200:
                continue
            info=future.result().json()['info']
            stats=future.result().json()['stats']

            if stats['calcTime'] is None and ('time' not in stats or stats['time']==''):
                continue
            tempo=round(time.time())
            if stats['calcTime'] is None:
                minutes=int(stats['time'][0:stats['time'].index(':')])
            else:
                tempo_jogo=round(stats['calcTime']['tm']/1000)
                minutes=math.floor((tempo-tempo_jogo)/60)
            if minutes>0:
                id=info['id']
                competition = live_events_aux[str(id)]

                if 'Women' in competition or 'women' in competition:
                    info['teams']['home'] = info['teams']['home'] + ' (W)'
                    info['teams']['away'] = info['teams']['away'] + ' (W)'
                team = info['teams']['home']
                game = info['teams']['home'] + ' - ' + info['teams']['away']
                score_home = int(stats['total']['T1'])
                score_away = int(stats['total']['T2'])
                attacks_h=int(stats['statistic']['T1']['attacks']) if 'attacks' in stats['statistic']['T1'] else 0
                attacks_a=int(stats['statistic']['T2']['attacks']) if 'attacks' in stats['statistic']['T2'] else 0
                dang_attacks_h=int(stats['statistic']['T1']['dangerous_attacks']) if 'dangerous_attacks' in stats['statistic']['T1'] else 0
                dang_attacks_a=int(stats['statistic']['T2']['dangerous_attacks']) if 'dangerous_attacks' in stats['statistic']['T2'] else 0

                shotson_h=int(stats['statistic']['T1']['on_target']) if 'on_target' in stats['statistic']['T1'] else 0
                shotson_a=int(stats['statistic']['T2']['on_target']) if 'on_target' in stats['statistic']['T2'] else 0
                shotsoff_h=int(stats['statistic']['T1']['off_target']) if 'off_target' in stats['statistic']['T1'] else 0
                shotsoff_a=int(stats['statistic']['T2']['off_target']) if 'off_target' in stats['statistic']['T2'] else 0
                possession_h=int(stats['statistic']['T1']['possession']) if 'possession' in stats['statistic']['T1'] else 0
                possession_a=int(stats['statistic']['T2']['possession']) if 'possession' in stats['statistic']['T2'] else 0
                reds_h=int(stats['statistic']['T1']['redcard']) if 'redcard' in stats['statistic']['T1'] else 0
                reds_a=int(stats['statistic']['T2']['redcard']) if 'redcard' in stats['statistic']['T2'] else 0


                statsp = calc_stats([attacks_h,attacks_a,dang_attacks_h,dang_attacks_a,shotson_h,shotson_a,shotsoff_h,shotsoff_a])

                urls_odds[id]='https://nodejs.tglab.io/cache/5/en/cz/'+str(id)+'/in-play-odds-by-event.json?hidenseek=a03912184ce56390b61e9f38a8ee7f512d5b5656e224'
                info_games[id]=[minutes,competition,team,game,score_home,score_away,attacks_h,attacks_a,dang_attacks_h,dang_attacks_a,shotson_h,shotson_a,shotsoff_h,shotsoff_a,possession_h,possession_a,reds_h,reds_a,statsp]

a=1

with concurrent.futures.ThreadPoolExecutor(max_workers=CONNECTIONS) as executor:
    future_to_url = (executor.submit(load_url, urls_odds[url], TIMEOUT) for url in urls_odds)
    for future in concurrent.futures.as_completed(future_to_url):
        if 1==1:
            found_g=False
            found_h=False
            found_1h=False
            odds_list=future.result().json()
            if odds_list['odds']=={}:
                continue

            info_game=False
            for odds in odds_list['odds']:
                detail=odds_list['odds'][odds]
                if not info_game:
                    [minutes, competition, team, game, score_home, score_away, attacks_h, attacks_a, dang_attacks_h,
                     dang_attacks_a, shotson_h, shotson_a, shotsoff_h, shotsoff_a, possession_h, possession_a, reds_h,
                     reds_a, statsp]=info_games[detail['event_id']]
                    handicap_diff = score_away - score_home
                    num_goals = score_away + score_home
                if detail['odd_code'] == 'ODD_HND_ASN_1_1':
                    a=1
                if detail['odd_code']=='ODD_HND_ASN_1_1' and float(detail['additional_value'][2:-1]).is_integer() and int(float(detail['additional_value'][2:-1]))==handicap_diff:
                    odd_home_h=detail['odd_value']
                    found_h = True
                elif detail['odd_code']=='ODD_HND_ASN_1_2' and float(detail['additional_value'][2:-1]).is_integer() and int(float(detail['additional_value'][2:-1]))==-handicap_diff:
                    odd_away_h = detail['odd_value']
                    found_h = True
                elif detail['odd_code'] == 'ODD_HND_ASN_HT1_1' and float(detail['additional_value'][2:-1]).is_integer() and int(float(detail['additional_value'][2:-1]))==handicap_diff:
                    odd_home_1h = detail['odd_value']
                    found_1h = True
                elif detail['odd_code']=='ODD_HND_ASN_HT1_2' and float(detail['additional_value'][2:-1]).is_integer() and int(float(detail['additional_value'][2:-1]))==-handicap_diff:
                    odd_away_1h = detail['odd_value']
                    found_1h = True
                elif num_goals==0 and detail['odd_code']=='ODD_FTB_FIRSTGOAL_HOME':
                    odd_home_g=detail['odd_value']
                    found_g=True
                elif num_goals == 0 and detail['odd_code'] == 'ODD_FTB_FIRSTGOAL_AWAY':
                    odd_away_g=detail['odd_value']
                    found_g=True
                elif num_goals > 0 and len(detail['odd_code']) == 21 and 'ODD_FTB_' in detail['odd_code'] and 'GOAL_HOME' in detail['odd_code'] and int(detail['odd_code'][9]) == num_goals+1:
                    odd_home_g = detail['odd_value']
                    found_g = True
                elif num_goals > 0 and len(detail['odd_code']) == 21 and 'ODD_FTB_' in detail['odd_code'] and 'GOAL_AWAY' in detail['odd_code'] and int(detail['odd_code'][9]) == num_goals+1:
                    odd_away_g = detail['odd_value']
                    found_g = True
            if found_g and (odd_home_g>1.001 and odd_away_g>1.001):
                oddsp_g = 100 * (1 / odd_home_g) / (1 / odd_home_g + 1 / odd_away_g)
            else:
                found_g=False
            if found_h and (odd_home_h>1.001 and odd_away_h>1.001):
                oddsp_h = 100 * (1 / odd_home_h) / (1 / odd_home_h + 1 / odd_away_h)
            else:
                found_h=False
            if found_1h and (odd_home_1h>1.001 and odd_away_1h>1.001):
                oddsp_1h = 100 * (1 / odd_home_1h) / (1 / odd_home_1h + 1 / odd_away_1h)
            else:
                found_1h=False

            if found_1h or found_h or found_g:
                odd_type=''
                if minutes<=25 and (found_g or found_1h):
                    if found_g:
                        odd_home = odd_home_g
                        odd_away = odd_away_g
                        oddsp = oddsp_g
                        odd_type = 'Next Goal'
                    else:
                        odd_home = odd_home_1h
                        odd_away = odd_away_1h
                        oddsp = oddsp_1h
                        odd_type = '1st half Handicap'
                elif minutes<=45 and (found_g or found_1h):
                    if (found_g and found_1h) and abs(oddsp_g - statsp) > abs(oddsp_1h - statsp)+5:
                        odd_home = odd_home_g
                        odd_away = odd_away_g
                        oddsp = oddsp_g
                        odd_type = 'Next Goal'
                    elif found_1h:
                        odd_home = odd_home_1h
                        odd_away = odd_away_1h
                        oddsp = oddsp_1h
                        odd_type = '1st half Handicap'
                    else:
                        odd_home = odd_home_g
                        odd_away = odd_away_g
                        oddsp = oddsp_g
                        odd_type = 'Next Goal'
                elif minutes<=65:
                    if found_g:
                        odd_home = odd_home_g
                        odd_away = odd_away_g
                        oddsp = oddsp_g
                        odd_type = 'Next Goal'
                    else:
                        odd_home = odd_home_h
                        odd_away = odd_away_h
                        oddsp = oddsp_h
                        odd_type='Final Handicap'
                else:
                    if found_h:
                        odd_home = odd_home_h
                        odd_away = odd_away_h
                        oddsp = oddsp_h
                        odd_type = 'Final Handicap'
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
        #except Exception as exc:
        #    data = str(type(exc))
        #    print("THERE WAS AN ERROR")


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
filename=curr_path+'\\Initial_Betsafe.txt'
with open(filename, 'w', newline='', encoding='utf-8') as f:
    movies = csv.writer(f,delimiter=';')
    movies.writerow(header)
    for x in live_events_detailed:
        movies.writerow(x)

filename=curr_path+'\\done.txt'
with open(filename, 'w', newline='') as f:
    f.write("done")
end=time.time()
print(end-start)
