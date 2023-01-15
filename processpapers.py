from collections import Counter
from datetime import date, datetime
import numpy as np
import pandas as pd
import requests
import pickle

repo_venueids = set([
    # BELOW: type=repository
    'V4306400194', # arxiv
    'V4306400562', # Zonodo
    'V4306402512', # HAL
    'V4306402144', # HAL
    'V4306400382', # HAL
    'V4306401953', # HAL
    'V4306400513', # HAL
    'V2734324842', # biorxiv
    'V4306401470', # HAL
    'V4306401056', # HAL
    'V4306401271', # RePEc
    'V4306402646', # HAL
    'V4306401715', # HAL
    'V4306400327', # HAL
    'V4306402567', # biorxiv
    'V4306400573', # medrxiv
    'V4306401481', # HAL
    'V4306402464', # HAL
    'V4306402368', # HAL
    # should we keep HAL? [yes]
    # BELOW: type=journal
    'V3121261024', # RePEc
    'V4210172589', # SSRN
    'V2751751161', # SSRN
    # PhilArchive, E-LIS and PsyArXiv are not included because OpenAlex lists few of their articles
])

misc_accepted_venues = set([
    'V4306401280', # DOAJ
    'V4306401840', # DergiPark
])

unaccepted_venues = set([
    'V4210172589', # SSRN
    'V2751751161', # SSRN
])

def get_vecs_nsamp_paperdfs_v9(lag_range_start, lag_range_end,
                            oth_range_start, oth_range_end,
                            real_range_start, real_range_end,
                            works_cnt_thres,
                            discip_id,
                            author_samples_d):

    # with open('./tmp.pkl', 'wb') as of:
    #     print('try pickle')
    #     pickle.dump((lag_range_start, lag_range_end,
    #                         oth_range_start, oth_range_end,
    #                         real_range_start, real_range_end,
    #                         works_cnt_thres,
    #                         discip_id,
    #                         author_samples_d), of)
    
    lag_range_start = int(lag_range_start + 0.5)
    lag_range_end = int(lag_range_end + 0.5)
    oth_range_start = int(oth_range_start + 0.5)
    oth_range_end = int(oth_range_end + 0.5)
    real_range_start = int(real_range_start + 0.5)
    real_range_end = int(real_range_end + 0.5)
    works_cnt_thres = int(works_cnt_thres + 0.5)
    
    print(f'start: discip {discip_id}')
    request_counter = Counter()

    def request_stubborn(url, type):
        retry_count = 0
        request_counter[type] += 1

        proxies = {'https':'127.0.0.1:7890'}
        
        while(True):
            try:
                response = requests.get(url, proxies = proxies).json()
            except:
                retry_count += 1
                print(f'retring the {retry_count}-th time\n\t{url}')
                continue

            break

        return response

    def venue_is_repo(venue_info):
        try:
            venue_id = venue_info['id'].replace('https://openalex.org/', '')
        except:
            return False
        return venue_id in repo_venueids
    
    def venue_accepted(venue_info):
        try:
            venue_id = venue_info['id'].replace('https://openalex.org/', '')
        except:
            return True
        
        if False:
            # use blacklist
            return venue_id not in unaccepted_venues
        else:
            # use whitelist
            return ((venue_info['type'] in ['journal', 'conference']) and (venue_id not in repo_venueids)) or (venue_id in misc_accepted_venues)
    
    def paper_validity(paper_info):
        if len(paper_info['referenced_works']) == 0:
            return False

        if venue_accepted(paper_info['host_venue']):
            return True
        
        for venue_num in range(len(paper_info['alternate_host_venues'])):
            if venue_accepted(paper_info['alternate_host_venues'][venue_num]):
                return True

        return False
    
    def paper_in_repo(paper_info):
        if venue_is_repo(paper_info['host_venue']):
            return True
        
        for venue_num in range(len(paper_info['alternate_host_venues'])):
            if venue_is_repo(paper_info['alternate_host_venues'][venue_num]):
                return True

        return False

    oth_len = oth_range_end - oth_range_start + 1
    lag_len = lag_range_end - lag_range_start + 1
    real_len = real_range_end - real_range_start + 1
    ppl_vec = np.zeros((oth_len, ), dtype = int)
    paper_vec = np.zeros((oth_len, ), dtype = int)
    tot_ath_vec = np.zeros((oth_len, ), dtype = int)
    tot_lag_vec = np.zeros((lag_len, ), dtype = int)
    nsamp_ath_d = 0
    paper_dfs = [pd.DataFrame({'id':[], 'year':[], 'rel2':[], 'cited_by_cnt':[], 'in_repo':[], 'n_authors':[], 'n_author_is_inexact':[]}, dtype = int) for _ in range(real_len)]
    outliers = []

    valid_papers = 0
    invalid_papers = 0
    noconcept_papers = 0

    giant_cnt_by_year = Counter()
    giant_sum_by_year = Counter()

    print(author_samples_d.head())

    for ath_num, ath_index in enumerate(author_samples_d.index):
        if ath_num % (len(author_samples_d) // 10) == 0:
            print(f'progress {ath_num}/{len(author_samples_d)}: \tvalid {valid_papers}  \tinvalid {invalid_papers}  \tno_concept {noconcept_papers}  \toutliers {len(outliers)}\n\t{request_counter}')

        ath_info = author_samples_d.loc[ath_index,]
        pub_years = []

        url_batch = f'http://api.openalex.org/works?filter=author.id:{ath_info["id"]},type:journal-article|proceedings-article&sort=publication_date:asc&per-page=200&cursor=*'
        cursor = '*'

        discard = False

        id2date = dict()

        while True:
            cur_url_batch = url_batch.replace('*', cursor)
            response = request_stubborn(cur_url_batch, 'batch')

            if cursor == '*':
                try:
                    if response['meta']['count'] < works_cnt_thres/2:
                        discard = True
                        outliers.append(ath_info['id'])
                        break
                    
                    if response['meta']['count'] > 1000:
                        url_groupby = f'https://api.openalex.org/works?filter=author.id:{ath_info["id"]},type:journal-article|proceedings-article&group_by=publication_year'
                        response_groupby = request_stubborn(url_groupby, 'groupby')
                        if response_groupby['group_by'][0]['count'] >= response['meta']['count']/2:
                            discard = True
                            outliers.append(ath_info['id'])
                            break
                except:
                    print(f'error {ath_info["id"]}')
                    discard = True
                    break

            cursor = response['meta']['next_cursor']

            for paper_num in range(len(response['results'])):
                paper_info = response['results'][paper_num]
                if not paper_validity(paper_info):
                    invalid_papers += 1
                    continue

                valid_papers += 1

                paper_id = paper_info['id'].replace('https://openalex.org/', '')
                paper_year = paper_info['publication_year']
                paper_date = datetime.strptime(paper_info['publication_date'], '%Y-%m-%d')
                paper_author_cnt = len(paper_info['authorships'])
                nath_inexact = 0 # False
                try:
                    if paper_info['is_authors_truncated'] == True:
                        if request_counter['single'] >= 1000 and ((real_range_start <= paper_year <= real_range_end) or (oth_range_start <= paper_year <= oth_range_end)):
                            nath_inexact = 1 # True
                            samp_cnt = 0 
                            samp_sum = 0
                            for dif in range(100):
                                samp_sum += giant_sum_by_year[paper_year - dif]
                                samp_cnt += giant_cnt_by_year[paper_year - dif]
                                if samp_cnt >= 80: break
                                if dif == 0: continue
                                samp_sum += giant_sum_by_year[paper_year + dif]
                                samp_cnt += giant_cnt_by_year[paper_year + dif]
                                if samp_cnt >= 80: break
                            
                            if samp_cnt < 80 or samp_cnt > 1000 or not (100 < (samp_sum/samp_cnt) < 300):
                                print(f'error: paper_year {paper_year}  \tsamp_cnt {samp_cnt}  \tsamp_sum {samp_sum}')
                            else:
                                paper_author_cnt = samp_sum / samp_cnt

                        elif request_counter['single'] < 1000:
                            url_single = f'https://api.openalex.org/works/{paper_id}'
                            response_single = request_stubborn(url_single, 'single')
                            paper_author_cnt = len(response_single['authorships'])
                            giant_cnt_by_year[paper_year] += 1
                            giant_sum_by_year[paper_year] += paper_author_cnt
                except:
                    pass

                pub_years.append(paper_year)
                
                if oth_range_start <= paper_year <= oth_range_end:
                    paper_vec[paper_year - oth_range_start] += 1
                    tot_ath_vec[paper_year - oth_range_start] += paper_author_cnt
                
                if paper_year >= lag_range_start:
                    for ref_num in range(len(paper_info['referenced_works'])):
                        ref_name = paper_info['referenced_works'][ref_num]
                        try:
                            assert('https://openalex.org/' in ref_name)
                            ref_id = ref_name.replace('https://openalex.org/', '')

                            if ref_id in id2date:
                                ref_date = id2date[ref_id][0]
                                ref_year = id2date[ref_id][1]
                                dif_days = min((paper_date - ref_date).days, 1096)
                                if dif_days > 0:
                                    tot_lag_vec[ref_year - lag_range_start] += dif_days
                                    del id2date[ref_id]
                        except:
                            print(f'error: reference {ref_name}')
                        
                    if paper_year <= lag_range_end:
                        id2date[paper_id] = (paper_date, paper_year)
                
                if real_range_start <= paper_year <= real_range_end:
                    num = len(paper_dfs[paper_year - real_range_start])
                    paper_dfs[paper_year - real_range_start].loc[num, 'n_authors'] = paper_author_cnt
                    paper_dfs[paper_year - real_range_start].loc[num, 'n_author_is_inexact'] = nath_inexact
                    paper_dfs[paper_year - real_range_start].loc[num, 'in_repo'] = (1 if paper_in_repo(paper_info) else 0)
                    paper_dfs[paper_year - real_range_start].loc[num, 'cited_by_cnt'] = paper_info['cited_by_count']
                    paper_dfs[paper_year - real_range_start].loc[num, 'year'] = paper_year
                    paper_dfs[paper_year - real_range_start].loc[num, 'id'] = paper_id

                    rel2 = 0
                    concept_found = False
                    for concept_num in range(len(paper_info['concepts'])):
                        concept_info = paper_info['concepts'][concept_num]
                        if concept_info['id'] == 'https://openalex.org/' + discip_id:
                            concept_found = True
                            rel2 = float(concept_info['score']) ** 2
                            break
                    if not concept_found:
                        noconcept_papers += 1
                    paper_dfs[paper_year - real_range_start].loc[num, 'rel2'] = rel2

            if not cursor:
                break
        
        if discard:
            continue
        
        nsamp_ath_d += 1

        for paper_id, paper_date_year in id2date.items():
            paper_year = paper_date_year[1]
            tot_lag_vec[paper_year - lag_range_start] += 1096
        
        if len(pub_years) > 0:
            start_year = max(oth_range_start, min(pub_years))
            end_year = min(oth_range_end, max(pub_years))
            if start_year <= end_year:
                ppl_vec[(start_year - oth_range_start):(end_year - oth_range_start + 1)] += 1
        else:
            print(f'error: {ath_info["id"]} has len(pub_years)==0')

    for i in range(len(paper_dfs)):
        paper_dfs[i].to_csv(f'./datatransfer/paper_df_{i+1}.csv')

    return (ppl_vec, paper_vec, tot_ath_vec, tot_lag_vec, nsamp_ath_d, len(paper_dfs), outliers)

if __name__ == "__main__":
    with open('./tmp.pkl', 'rb') as f:
        args = pickle.load(f)
    
    get_vecs_nsamp_paperdfs_v9(*args)