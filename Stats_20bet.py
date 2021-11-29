import concurrent.futures
import csv
import os
import pickle
import time

import requests


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
    if (data['sport_id']=='1' or data['sport_id']==1) and '-srl' not in data['translationSlug'] and 'Simulated' not in sport_categories[data['sportCategoryId']]:
        return True
    else:
        return False



start=time.time()
curr_path=os.path.dirname(os.path.abspath(__file__))
filename=curr_path+'\\prev20bet.pickle'
if os.path.exists(filename):
    with open(filename, 'rb') as f:
        [prev,prev_time] = pickle.load(f)
else:
    prev=[]
    prev_time=[]

main_data=requests.get('https://platform.20bet.com/api/v2/menu/live/en').json()['data']
live_events_aux=main_data['events']
sport_categories={}
for i in main_data['sportCategories']:
    sport_categories[i['id']]=i['name']


urls=[]
live_events={}
live_events_detailed=[]
num_games=0
for i in live_events_aux:
    if real_game(i):
        if 1==1:
            num_games += 1
            live_events[i['vendorEventId'].split(':')[-1]] = {}
            urls.append('https://platform.20bet.com/api/event/list?eventId_eq=' + str(i['id']) + '&main=0&relations%5B%5D=league&relations%5B%5D=odds&relations%5B%5D=result&relations%5B%5D=withMarketsCount&relations%5B%5D=competitors&relations%5B%5D=sportCategories&relations%5B%5D=players&relations%5B%5D=broadcasts&lang=en')
            urls.append('https://lmt.fn.sportradar.com/common/en/Etc:UTC/gismo/match_detailsextended/' + str(i['vendorEventId'].split(':')[-1]))
            urls.append('https://lmt.fn.sportradar.com/common/en/Etc:UTC/gismo/match_timeline/' + str(i['vendorEventId'].split(':')[-1]))

print('Number of games: ' + str(num_games) + ' expected')

CONNECTIONS = len(urls)
CONNECTIONS = 100
TIMEOUT = 20
football_events=[]

with concurrent.futures.ThreadPoolExecutor(max_workers=CONNECTIONS) as executor:
    future_to_url = (executor.submit(load_url, url, TIMEOUT) for url in urls)
    for future in concurrent.futures.as_completed(future_to_url):
        try:
            if future._result.url[:10]=='https://pl':
                data = future.result().json()['data']['relations']
                matchid=future.result().json()['data']['items'][0]['vendorEventId'].split(':')[-1]
                localid=str(future.result().json()['data']['items'][0]['id'])

                if future.result().json()['data']['items'][0]['competitor1Id']==data['competitors'][0]['id']:
                    live_events[matchid]['team'] = data['competitors'][0]['name']
                    live_events[matchid]['game'] = data['competitors'][0]['name'] + ' - ' + data['competitors'][1]['name']
                else:
                    live_events[matchid]['team'] = data['competitors'][1]['name']
                    live_events[matchid]['game'] = data['competitors'][1]['name'] + ' - ' + data['competitors'][0]['name']
                football_events.append(live_events[matchid]['game'])

                if data['sportCategories'][0]['name']=='':
                    live_events[matchid]['competition'] = data['league'][0]['name']
                else:
                    live_events[matchid]['competition'] = data['sportCategories'][0]['name'] + '/' + data['league'][0]['name']
                minutes=int(data['result'][localid]['clock']['matchTime'].split(':')[0])
                live_events[matchid]['minutes']=minutes

                score_home=data['result'][localid]['team1Score']
                score_away=data['result'][localid]['team2Score']
                hcp_str='hcp=' + str(score_away-score_home)
                live_events[matchid]['found_g'] = False
                live_events[matchid]['found_h'] = False

                if len(data['odds'])>0:
                    if minutes < 43:
                        for betoffer in data['odds'][localid]:
                            if betoffer['outcomes']!=None:
                                if ((betoffer['id'] == 836 and betoffer['specifiers']==hcp_str) or (betoffer['id'] == 794 and score_home==score_away)) and betoffer['outcomes'][0]['odds']!=None and betoffer['outcomes'][1]['odds']!=None:  # handicap
                                    odd_home_h=betoffer['outcomes'][0]['odds']
                                    odd_away_h=betoffer['outcomes'][1]['odds']
                                    live_events[matchid]['odd_home_h'] = odd_home_h
                                    live_events[matchid]['odd_away_h'] = odd_away_h
                                    live_events[matchid]['oddsp_h'] = 100 * (1 / odd_home_h) / (1 / odd_home_h + 1 / odd_away_h)
                                    live_events[matchid]['found_h'] = True
                    for betoffer in data['odds'][localid]:
                        if betoffer['outcomes']!=None:
                            if betoffer['id']==435 and betoffer['outcomes'][0]['odds']!=None and betoffer['outcomes'][2]['odds']!=None and (betoffer['outcomes'][0]['odds']<6 or betoffer['outcomes'][2]['odds']<6): #nextgoal
                                odd_home_g = betoffer['outcomes'][0]['odds']
                                odd_away_g = betoffer['outcomes'][2]['odds']
                                live_events[matchid]['odd_home_g'] = odd_home_g
                                live_events[matchid]['odd_away_g'] = odd_away_g
                                live_events[matchid]['oddsp_g'] = 100 * (1 / odd_home_g) / (1 / odd_home_g + 1 / odd_away_g)
                                live_events[matchid]['found_g'] = True
                            if betoffer['id']==557 and betoffer['specifiers']==hcp_str and minutes >= 45 and betoffer['outcomes'][0]['odds']!=None and betoffer['outcomes'][1]['odds']!=None and (betoffer['outcomes'][0]['odds']<3 or betoffer['outcomes'][1]['odds']<3):  # handicap
                                odd_home_h = betoffer['outcomes'][0]['odds']
                                odd_away_h = betoffer['outcomes'][1]['odds']
                                live_events[matchid]['odd_home_h'] = odd_home_h
                                live_events[matchid]['odd_away_h'] = odd_away_h
                                live_events[matchid]['oddsp_h'] = 100 * (1 / odd_home_h) / (1 / odd_home_h + 1 / odd_away_h)
                                live_events[matchid]['found_h'] = True

                #
                # for betoffer in data['odds'][localid]:
                #     if betoffer['id']==435: #nextgoal
                #         a=1
                #     if betoffer['id']==557 and betoffer['specifiers']==hcp_str: #handicap
                #         b=1
                #     if betoffer['id'] == 836 and betoffer['specifiers']==hcp_str:  # 1st half handicap
                #         b = 1
                #     if betoffer['id'] == 794 and score_home==score_away:  # 1st half draw no bet
                #         b = 1
                # b=1

            elif 'match_detailsextended' in future._result.url:
                data = future.result().json()['doc'][0]['data']
                matchid=str(data['_matchid'])
                if 'values' in data:
                    live_events[matchid]['possession_h']=data['values']['110']['value']['home'] if '110' in data['values'] else 0
                    live_events[matchid]['possession_a']=data['values']['110']['value']['away'] if '110' in data['values'] else 0
                    live_events[matchid]['reds_h']=data['values']['50']['value']['home'] if '50' in data['values'] else 0
                    live_events[matchid]['reds_a']=data['values']['50']['value']['away'] if '50' in data['values'] else 0
                    live_events[matchid]['reds_h']+=data['values']['45']['value']['home'] if '45' in data['values'] else 0
                    live_events[matchid]['reds_a']+=data['values']['45']['value']['away'] if '45' in data['values'] else 0
                    live_events[matchid]['attacks_h']=data['values']['1126']['value']['home'] if '1126' in data['values'] else 0
                    live_events[matchid]['attacks_a']=data['values']['1126']['value']['away'] if '1126' in data['values'] else 0
                    live_events[matchid]['dang_attacks_h']=data['values']['1029']['value']['home'] if '1029' in data['values'] else 0
                    live_events[matchid]['dang_attacks_a']=data['values']['1029']['value']['away'] if '1029' in data['values'] else 0
                else:
                    live_events[matchid]['possession_h'] = 0
                    live_events[matchid]['possession_a'] = 0
                    live_events[matchid]['reds_h'] = 0
                    live_events[matchid]['reds_a'] = 0
                    live_events[matchid]['attacks_h'] = 0
                    live_events[matchid]['attacks_a'] = 0
                    live_events[matchid]['dang_attacks_h'] = 0
                    live_events[matchid]['dang_attacks_a'] = 0
            else:
                data = future.result().json()['doc'][0]['data']
                matchid=str(data['match']['_id'])

                live_events[matchid]['score_home']=data['match']['result']['home']
                live_events[matchid]['score_away']=data['match']['result']['away']

                if live_events[matchid]['score_home']==None:
                    live_events[matchid]['shotson_h'] = 0
                    live_events[matchid]['shotson_a'] = 0
                else:
                    live_events[matchid]['shotson_h'] = live_events[matchid]['score_home']
                    live_events[matchid]['shotson_a'] = live_events[matchid]['score_away']

                live_events[matchid]['shotsoff_h']=0
                live_events[matchid]['shotsoff_a']=0
                for i in data['events']:
                    if i['type']=='shotofftarget' or i['type']=='shotblocked':
                        if i['team']=='home':
                            live_events[matchid]['shotsoff_h']+=1
                        elif i['team']=='away':
                            live_events[matchid]['shotsoff_a'] += 1
                    elif i['type']=='shotontarget':
                        if i['team'] == 'home':
                            try:
                                live_events[matchid]['shotson_h'] += 1
                            except:
                                a=1
                        elif i['team'] == 'away':
                            live_events[matchid]['shotson_a'] += 1
        except Exception as exc:
            data = str(type(exc))
            print("THERE WAS AN ERROR")

for i in live_events:
    data=live_events[i]
    try:
        statsp = calc_stats([data['attacks_h'], data['attacks_a'], data['dang_attacks_h'], data['dang_attacks_a'], data['shotson_h'], data['shotson_a'], data['shotsoff_h'], data['shotsoff_a']])
        goodstats=1
    except:
        goodstats=0

    if (data['found_h'] or data['found_g']) and goodstats==1:
        odd_type=''
        if data['found_h'] and (data['minutes']>=70 or (data['minutes']>=25 and data['minutes']<45)) :
            odd_home = data['odd_home_h']
            odd_away = data['odd_away_h']
            oddsp = data['oddsp_h']
            if data['minutes']<45:
                odd_type='1st half Handicap'
            else:
                odd_type='Final Handicap'
        elif not data['found_g'] and data['found_h']:
            odd_home = data['odd_home_h']
            odd_away = data['odd_away_h']
            oddsp = data['oddsp_h']
            if data['minutes']<45:
                odd_type='1st half Handicap'
            else:
                odd_type='Final Handicap'
        else:
            odd_home = data['odd_home_g']
            odd_away = data['odd_away_g']
            oddsp = data['oddsp_g']
            odd_type = 'Next Goal'

        if statsp == -100:
            live_events_detailed.append([data['minutes'], data['competition'], data['game'], oddsp, 0, '',odd_home, odd_away,data['attacks_h'],data['attacks_a'],data['dang_attacks_h'],data['dang_attacks_a'],data['shotson_h'],data['shotson_a'],data['shotsoff_h'],data['shotsoff_a'],data['possession_h'],data['possession_a'],data['score_home'],data['score_away'],data['reds_h'],data['reds_a'],odd_type])
        else:
            live_events_detailed.append([data['minutes'], data['competition'], data['game'], oddsp, statsp, oddsp - statsp,odd_home, odd_away,data['attacks_h'],data['attacks_a'],data['dang_attacks_h'],data['dang_attacks_a'],data['shotson_h'],data['shotson_a'],data['shotsoff_h'],data['shotsoff_a'],data['possession_h'],data['possession_a'],data['score_home'],data['score_away'],data['reds_h'],data['reds_a'],odd_type])
    else:
        if data['minutes']<89:
            print("No bet for game in " + str(data['minutes']) + "min: " + data['game'])


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
filename=curr_path+'\\Initial20bet.txt'
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
