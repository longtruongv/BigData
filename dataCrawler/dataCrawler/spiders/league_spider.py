import scrapy
from scrapy import Selector
from scrapy.http import Request, Response
import re
from dataCrawler.items import GeneralItem
import dataCrawler.utils as utils

LEAGUE_LIST = [
    '/en/comps/9/history/Premier-League-Seasons',
    '/en/comps/12/history/La-Liga-Seasons',
    '/en/comps/11/history/Serie-A-Seasons',
    '/en/comps/13/history/Ligue-1-Seasons',
    '/en/comps/20/history/Bundesliga-Seasons',
]

class LeagueSpider(scrapy.Spider):
    name = 'league'

    allowed_domains = ['fbref.com']
    based_url = 'https://fbref.com'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def start_requests(self):
        for league_uri in LEAGUE_LIST:
            url = f'{self.based_url}{league_uri}'

            yield Request(
                url=url,
                callback=self.parse_league,
            )

    def parse_league(self, response: Response, **kwargs):
        selector = Selector(response)
        season_uri_list = selector.xpath('//*[@data-stat="league_name"]/a/@href').extract()
        print('check', season_uri_list)
        for season_uri in season_uri_list:
            league_id_season = self.extract_id_season(season_uri)
            url = f'{self.based_url}{season_uri}'

            yield Request(
                url=url,
                callback=self.parse_league_season,
                meta={'id_season': league_id_season},
            )

    def parse_league_season(self, response: Response, **kwargs):
        selector = Selector(response)
        
        id, season = response.meta.get('id_season')

        item = GeneralItem()
        item['_id'] = id + "##" + season
        item['type'] = 'league'
        item['info'] = self.extract_info(selector)
        item['stats'] = self.extract_stats(selector)

        yield item

    #######################
    ## EXTRACT FROM DATA ##
    #######################

    def extract_id_season(self, uri: str):
        try:
            elements = uri.split('/')
            idx = elements.index('comps')
            return (elements[idx+1], elements[idx+2])
        except:
            return None, None

    def extract_info(self, selector: Selector):
        selector_ = selector.xpath('//*[@id="meta"]/div')

        if selector_[0].xpath('.//@class').extract_first() == 'media-item logo loader':
            selector = selector_[1]
        else:
            selector = selector_[0]

        leaguename = selector.xpath('.//h1/text()').extract()
        s = leaguename[0]
        s = re.sub('\s+', '', s)
        return {
            'LeagueName': s,
        }

    def extract_stats(self, selector: Selector):
        # STANDARD SQUAD STATS
        standard_table = selector.xpath('//*[@id="stats_squads_standard_for"]')
        standard = self.extract_stats_table(standard_table)

        # STANDARD PLAYER STATS
        player_table = selector.xpath('//*[@id="stats_standard"]')
        print('test',player_table)
        player = self.extract_stats_table(player_table)

        # SHOOTING STATS
        shooting_table = selector.xpath('//*[@id="stats_squads_shooting_for"]')
        shooting = self.extract_stats_table(shooting_table)

        # PASSING STATS
        passing_table = selector.xpath('//*[@id="stats_squads_passing_for"]')
        passing = self.extract_stats_table(passing_table)

        # GOAL AND SHOT CREATION
        goal_shot_table = selector.xpath('//*[@id="stats_squads_gca_for"]')
        goal_shot = self.extract_stats_table(goal_shot_table)

        # PLAYING TIME
        playtime_table = selector.xpath('//*[@id="stats_squads_playing_time_for"]')
        playtime = self.extract_stats_table(playtime_table)

        # GOALKEEPING
        goalkeeping_table = selector.xpath('//*[@id="stats_squads_keeper_for"]')
        goalkeeping = self.extract_stats_table(goalkeeping_table)


        return {
            'std': standard,
            'player_stats':player,
            'shooting': shooting,
            'passing': passing,
            # 'pass_type': pass_type,
            'goal_shot_creation': goal_shot,
            # 'defensive': defensive,
            # 'possession': possession,
            'playing_time': playtime,
            # 'miscellaneous': miscellaneous,
            'goalkeeping': goalkeeping,
            # 'adv_goalkeeping': adv_goalkeeping,
            # 'scores_fixture': scores_fixture,
        }

    def extract_stats_table(self, table_selector: Selector):
        if not table_selector:
            return None

        headers = table_selector.xpath('.//thead/tr[last()]/th/text()').extract()
        rows = table_selector.xpath('.//tbody/tr')

        all_data = []
        for row in rows:
            check = []
            cells = row.xpath('.//th|.//td')
            data = {}
            for idx, cell in enumerate(cells):
                if cell.xpath('.//@class').extract_first() in ['thead', 'over_header thead']:
                    print(cell.xpath('.//text()').extract_first())
                    continue
                if cell.xpath('.//a').extract_first():
                    # text = cell.xpath('.//a/text()').extract_first()
                    id_in_href = utils.parse_id_from_uri(cell.xpath('.//a/@href').extract_first())
                    if headers[idx] not in check:
                        data[headers[idx]] = id_in_href
                        check.append(headers[idx])
                    else:
                        headers[idx] = headers[idx] + '2'
                        data[headers[idx]] = id_in_href
                else:
                    if headers[idx] not in check:
                        data[headers[idx]] = utils.parse_num(cell.xpath('.//text()').extract_first())
                        check.append(headers[idx])
                    else:
                        headers[idx] = headers[idx] + '2'
                        data[headers[idx]] = utils.parse_num(cell.xpath('.//text()').extract_first())
            all_data.append(data)
        return all_data