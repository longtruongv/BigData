import scrapy
from scrapy import Selector
from scrapy.http import Request, Response

from dataCrawler.items import GeneralItem

LEAGUE_LIST = [
    '/en/comps/9/Premier-League-Stats',
    # '/en/comps/12/La-Liga-Stats',
    # '/en/comps/11/Serie-A-Stats',
    # '/en/comps/13/Ligue-1-Stats',
    # '/en/comps/20/Bundesliga-Stats',
]

class ClubSpider(scrapy.Spider):
    name = "club"

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
        club_uri_list = selector.xpath('//div[@id="all_stats_squads_standard"]//table/tbody/tr/th/a/@href').extract()

        for club_uri in club_uri_list:
            club_id = self.extract_id(club_uri)
            url = f'{self.based_url}{club_uri}'

            yield Request(
                url=url,
                callback=self.parse_club,
                meta={'id': club_id}
            )

    def parse_club(self, response: Response, **kwargs):
            selector = Selector(response)
            
            item = GeneralItem()
            item['_id'] = response.meta.get('id')
            item['type'] = 'club'
            item['info'] = self.extract_info(selector)
            item['stats'] = self.extract_stats(selector)

            yield item


    #######################
    ## EXTRACT FROM DATA ##
    #######################

    def extract_id(self, uri: str):
        try:
            elements = uri.split('/')
            return elements[-2]
        except:
            return None

    def extract_info(self, selector: Selector):
        selector_ = selector.xpath('//*[@id="meta"]/div')

        if selector_[0].xpath('.//@class').extract_first() == 'media-item logo loader':
            selector = selector_[1]
        else:
            selector = selector_[0]

        clubname = selector.xpath('.//h1/span/text()').extract_first()
        # SỬA REGEX
         
        return {
            'ClubName': clubname,
        }

    def extract_stats(self, selector: Selector):
        # STANDARD STATS
        standard_table = selector.xpath('//*[@id="stats_shooting_12"] | //*[@id="stats_shooting_11"] | //*[@id="stats_shooting_13"] | //*[@id="stats_shooting_9"] | //*[@id="stats_shooting_20"]')
        standard = self.extract_stats_table(standard_table)

        # SHOOTING STATS
        shooting_table = selector.xpath('//*[@id="stats_shooting_12"] | //*[@id="stats_shooting_11"] | //*[@id="stats_shooting_13"] | //*[@id="stats_shooting_9"] | //*[@id="stats_shooting_20"]')
        shooting = self.extract_stats_table(shooting_table)

        # PASSING STATS
        passing_table = selector.xpath('//*[@id="stats_passing_12"] | //*[@id="stats_passing_11"] | //*[@id="stats_passing_13"] | //*[@id="stats_passing_9"] | //*[@id="stats_passing_20"]')
        passing = self.extract_stats_table(passing_table)

        # GOAL AND SHOT CREATION
        goal_shot_table = selector.xpath('//*[@id="stats_gca_12"] | //*[@id="stats_gca_11"] | //*[@id="stats_gca_13"] | //*[@id="stats_gca_9"] | //*[@id="stats_gca_20"]')
        goal_shot = self.extract_stats_table(goal_shot_table)

        # PLAYING TIME
        playtime_table = selector.xpath('//*[@id="stats_playing_time_12"] | //*[@id="stats_playing_time_9"] | //*[@id="stats_playing_time_11"] | //*[@id="stats_playing_time_13"] | //*[@id="stats_playing_time_20"]')
        playtime = self.extract_stats_table(playtime_table)

        # SCORES FIXTURE
        scores_fixture_table = selector.xpath('//*[@id="matchlogs_for"]')
        scores_fixture = self.extract_stats_table(scores_fixture_table)

        return {
            'std': standard,
            'shooting': shooting,
            'passing': passing,
            # 'pass_type': pass_type,
            'goal_shot_creation': goal_shot,
            # 'defensive': defensive,
            # 'possession': possession,
            'playing_time': playtime,
            # 'miscellaneous': miscellaneous,
            # 'goalkeeping': goalkeeping,
            # 'adv_goalkeeping': adv_goalkeeping,
            'scores_fixture': scores_fixture,
        }

    def extract_stats_table(self, table_selector: Selector):
        if not table_selector:
            return None

        headers = table_selector.xpath('.//thead/tr[last()]/th/text()').extract()
        rows = table_selector.xpath('.//tbody/tr')

        all_data = []
        for row in rows:
            cells = row.xpath('.//th|.//td')
            data = {}
            for idx, cell in enumerate(cells):
                if cell.xpath('.//@class').extract_first() in ['thead', 'over_header thead']:
                    print(cell.xpath('.//text()').extract_first())
                    continue
                if cell.xpath('.//a').extract_first():
                    # text = cell.xpath('.//a/text()').extract_first()
                    href = cell.xpath('.//a/@href').extract_first()
                    data[headers[idx]] = href
                else:
                    data[headers[idx]] = cell.xpath('.//text()').extract_first()     
            all_data.append(data)
        return all_data