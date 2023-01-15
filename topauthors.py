from collections import Counter
import requests
import numpy as np
import pandas as pd

# def test_func(discips):
#     print(discips)
#     return np.array([[1e18,4],[2,3]], dtype = np.int64)

def get_top_authors(discips, thres):
    thres = int(thres + 0.5)

    concept_id2num = dict()
    for num, oa_id in enumerate(discips['id_nourl']):
        concept_id2num[oa_id] = num + 1
    
    print(f'thres = {thres}')
    print(concept_id2num)

    url_batch = f'https://api.openalex.org/authors?filter=works_count:>{thres-1}&per-page=200&cursor=*'
    cursor = '*'

    url_single = 'https://api.openalex.org/works?filter=author.id:XXX,type:journal-article|proceedings-article'

    # results = np.zeros(shape = (540000, 3), dtype = np.int64)
    # author_cnt = 0
    raw_author_cnt = 0
    raw_author_total = 0
    results = pd.DataFrame({'id':[], 'works_cnt':[], 'discip':[]})
    page_cnt = 0

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


    while(True):
        cur_url_batch = url_batch.replace('*', cursor)
        response = request_stubborn(cur_url_batch, 'batch')

        cursor = response['meta']['next_cursor']

        if raw_author_total == 0:
            raw_author_total = response['meta']['count']

        page_cnt += 1
        if page_cnt % 50 == 1 or (not cursor):
            print(f'[{"%.2f"%(raw_author_cnt/raw_author_total,)}] starting the {page_cnt}-th page, got {len(results)} authors...\n\t{request_counter}\n\t{results.memory_usage(deep=True).sum()} Bytes')

        for author in range(len(response['results'])):
            raw_author_cnt += 1

            author_info = response['results'][author]
            works_cnt = author_info['works_count']
            
            discard = False
            try:
                oa_id = author_info['id']
                oa_id = oa_id.replace('https://openalex.org/', '')
                assert oa_id[0] == 'A'

                if works_cnt >= 5000:
                    cur_url_single = url_single.replace('XXX', oa_id)
                    response_single = request_stubborn(cur_url_single, 'single')
                    new_works_cnt = response_single['meta']['count']
                    if new_works_cnt < thres:
                        print(f'{oa_id}: {works_cnt} -> {new_works_cnt}, discarded')
                        discard = True
                    
                    works_cnt = new_works_cnt

            except:
                discard = True
            
            if discard:
                continue
            
            author_discip = -1
            for concept in range(len(author_info['x_concepts'])):
                if author_info['x_concepts'][concept]['level'] == 0:
                    concept_id = author_info['x_concepts'][concept]['id']
                    concept_id = concept_id.replace('https://openalex.org/', '')
                    assert concept_id in concept_id2num
                    author_discip = concept_id2num[concept_id]
                    break
            
            if author_discip != -1:
                num = len(results)
                results.loc[num, 'id'] = oa_id
                results.loc[num, 'works_cnt'] = works_cnt
                results.loc[num, 'discip'] = author_discip

        if not cursor:
            break
    
    return results

if __name__ == "__main__":
    discips = pd.DataFrame({'id_nourl':['C41008148','C71924100','C86803240','C121332964','C17744445','C185592680','C138885662','C127413603','C33923547','C15744967','C192562407','C142362112','C205649164','C144133560','C162324750','C144024400','C127313418','C39432304','C95457728']})
    print(get_top_authors(discips, 105))