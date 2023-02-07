import scrapy
from scrapy import Selector
from scrapy.http import Request, Response

from dataCrawler.items import GeneralItem
import dataCrawler.utils as utils

# COUNTRY_LIST = [
#     '/en/country/players/ENG/England-Football-Players',
#     '/en/country/players/GER/Germany-Football-Players',
#     '/en/country/players/ESP/Spain-Football-Players',
#     '/en/country/players/FRA/France-Football-Players',
#     '/en/country/players/POR/Portugal-Football-Players',
#     '/en/country/players/BRA/Brazil-Football-Players',
#     '/en/country/players/ARG/Argentina-Football-Players',
#     '/en/country/players/ITA/Italy-Football-Players'
# ]

LEAGUE_LIST = [
    # '/en/comps/9/Premier-League-Stats',
    '/en/comps/12/La-Liga-Stats',
    '/en/comps/11/Serie-A-Stats',
    '/en/comps/13/Ligue-1-Stats',
    '/en/comps/20/Bundesliga-Stats',
]

class PlayerSpider(scrapy.Spider):
    name = "player"

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
            club_uri = self.club_uri_transform(club_uri)
            url = f'{self.based_url}{club_uri}'

            yield Request(
                url=url,
                callback=self.parse_club,
            )

    def parse_club(self, response: Response, **kwargs):
        selector = Selector(response)
        player_uri_list = selector.xpath('//*[@id="content"]//h2/a/@href').extract()

        for player_uri in player_uri_list:
            player_id = self.extract_id(player_uri)
            url = f'{self.based_url}/{player_uri}'

            yield Request(
                url=url,
                callback=self.parse_player,
                meta={'id': player_id}
            )


    def parse_country(self, response: Response, **kwargs):
        selector = Selector(response)
        player_selector_list = selector.xpath('//*[@class="section_content"]/p')

        for player_selector in player_selector_list[:]:
            if not self.filter_by_year(player_selector):
                continue

            player_uri = player_selector.xpath('//*[@class="section_content"]/p/a/@href').extract_first()
            player_id = self.extract_id(player_uri)
            url = f'{self.based_url}/{player_uri}'

            yield Request(
                url=url,
                callback=self.parse_player,
                meta={'id': player_id}
            )

    def parse_player(self, response: Response, **kwargs):
        selector = Selector(response)

        item = GeneralItem()
        item['_id'] = response.meta.get('id')
        item['type'] = 'player'
        item['info'] = self.extract_info(selector)
        item['stats'] = self.extract_stats(selector)

        yield item


    #######################
    ## EXTRACT FROM DATA ##
    #######################

    def club_uri_transform(self, uri: str):
        # from: '/en/squads/19538871/Manchester-United-Stats'
        # to:   '/en/squads/19538871/2022-2023/roster/Manchester-United-Roster-Details'

        uri = uri.replace('Stats', 'Roster-Details')
        elements = uri.split('/')
        elements.insert(4, '2022-2023/roster')
        return '/'.join(elements)

    def filter_by_year(self, selector: Selector):
        player_data = selector.xpath('//*[@class="section_content"]/p/text()').extract_first()
        year_data = player_data.split('·')[0].strip()
        year_data = year_data.split('-')
        
        try: 
            if (len(year_data) > 1):
                start_year = int(year_data[0])
                end_year = int(year_data[1])

            if start_year >= 2010:
                print('start', start_year)
                return True
            elif end_year >= 2020:
                print('end', end_year)
                return True
            return False
        except:
            return False

    def extract_id(self, uri: str):
        try:
            elements = uri.split('/')
            idx = elements.index('players')
            return elements[idx+1]
        except:
            return None


    def extract_info(self, selector: Selector):
        if 'media-item' in selector.xpath('//*[@id="meta"]/div[1]/@class').extract_first():
            curr_xpath = '//*[@id="meta"]/div[2]'
        else:
            curr_xpath = '//*[@id="meta"]/div[1]'

        selector = selector.xpath(curr_xpath)

        shortname = selector.xpath(curr_xpath + '/h1/span/text()').extract_first()

        offset = 1
        if 'Position:' in selector.xpath(curr_xpath + '/p[1]/strong/text()').extract_first():
            fullname = None
            remain_offset_len = len(selector.xpath(curr_xpath + '/p').extract())
        else:
            fullname = selector.xpath(curr_xpath + '/p[1]/strong/text()').extract_first()
            offset += 1
            remain_offset_len = len(selector.xpath(curr_xpath + '/p').extract()) - 1


        position, footed, height, weight, dob, nation, club = None, None, None, None, None, None, None

        for i in range(remain_offset_len):
            temp_offset = offset + i

            if len(selector.xpath(curr_xpath + f'/p[{temp_offset}]/strong').extract()) == 0:
                height = selector.xpath(curr_xpath + f'/p[{temp_offset}]/span[1]/text()').extract_first()
                weight = selector.xpath(curr_xpath + f'/p[{temp_offset}]/span[2]/text()').extract_first()

            elif 'Position:' in selector.xpath(curr_xpath + f'/p[{temp_offset}]/strong/text()').extract_first():
                position = selector.xpath(curr_xpath + f'/p[{temp_offset}]/text()[1]').extract_first()
                footed = selector.xpath(curr_xpath + f'/p[{temp_offset}]/text()[2]').extract_first()

            elif 'Born:' in selector.xpath(curr_xpath + f'/p[{temp_offset}]/strong/text()').extract_first():
                dob = selector.xpath(curr_xpath + f'/p[{temp_offset}]/span/@data-birth').extract_first()

            elif any(
                item in selector.xpath(curr_xpath + f'/p[{temp_offset}]/strong/text()').extract_first() 
                for item in ['National Team:', 'Citizenship:']
            ):
                nation = utils.parse_id_from_uri(selector.xpath(curr_xpath + f'/p[{temp_offset}]/a/@href').extract_first())

            elif 'Club:' in selector.xpath(curr_xpath + f'/p[{temp_offset}]/strong/text()').extract_first():
                club = utils.parse_id_from_uri(selector.xpath(curr_xpath + f'/p[{temp_offset}]/a/@href').extract_first())

        return {
            'ShortName': shortname,
            'FullName': fullname,
            'Position': position,
            'Height': height,
            'Weight': weight,
            'Footed': footed,
            'DOB': dob,
            'Nationality': nation,
            'Club': club,
        }


    def extract_stats(self, selector: Selector):
        # STANDARD STATS
        standard_table = selector.xpath('//*[@id="stats_standard_dom_lg"]')
        standard = self.extract_stats_table(standard_table)

        # SHOOTING STATS
        shooting_table = selector.xpath('//*[@id="stats_shooting_dom_lg"]')
        shooting = self.extract_stats_table(shooting_table)

        # PASSING STATS
        passing_table = selector.xpath('//*[@id="stats_passing_dom_lg"]')
        passing = self.extract_stats_table(passing_table)

        # PASS TYPE
        pass_type_table = selector.xpath('//*[@id="stats_passing_types_dom_lg"]')
        pass_type = self.extract_stats_table(pass_type_table)

        # GOAL AND SHOT CREATION
        goal_shot_table = selector.xpath('//*[@id="stats_gca_dom_lg"]')
        goal_shot = self.extract_stats_table(goal_shot_table)

        # DEFENSICE ACTIONS
        defensive_table = selector.xpath('//*[@id="stats_defense_dom_lg"]')
        defensive = self.extract_stats_table(defensive_table)
 
        # POSSESSION
        possession_table = selector.xpath('//*[@id="stats_possession_dom_lg"]')
        possession = self.extract_stats_table(possession_table)

        # PLAYING TIME
        playtime_table = selector.xpath('//*[@id="stats_playing_time_dom_lg"]')
        playtime = self.extract_stats_table(playtime_table)

        # MISCELLANEOUS
        miscellaneous_table = selector.xpath('//*[@id="stats_misc_dom_lg"]')
        miscellaneous = self.extract_stats_table(miscellaneous_table)

        # GOALKEEPING
        goalkeeping_table = selector.xpath('//*[@id="stats_keeper_dom_lg"]')
        goalkeeping = self.extract_stats_table(goalkeeping_table)

        # ADVANCED GOALKEEPING
        adv_goalkeeping_table = selector.xpath('//*[@id="stats_keeper_adv_dom_lg"]')
        adv_goalkeeping = self.extract_stats_table(adv_goalkeeping_table)

        return {
            'std': standard,
            'shooting': shooting,
            'passing': passing,
            'pass_type': pass_type,
            'goal_shot_creation': goal_shot,
            'defensive': defensive,
            'possession': possession,
            'playing_time': playtime,
            'miscellaneous': miscellaneous,
            'goalkeeping': goalkeeping,
            'adv_goalkeeping': adv_goalkeeping,
        }


    def extract_stats_table(self, table_selector: Selector):
        if not table_selector:
            return None

        headers = table_selector.xpath('.//thead/tr[last()]/th/text()').extract()
        rows = table_selector.xpath('.//tbody/tr[@id="stats"]')

        all_data = []
        for row in rows:
            check = []
            cells = row.xpath('.//th|.//td')
            data = {}
            for idx, cell in enumerate(cells):
                if cell.xpath('.//@class').extract_first() in ['thead', 'over_header thead']:
                    # print(cell.xpath('.//text()').extract_first())
                    continue
                if cell.xpath('.//a').extract_first():
                    # text = cell.xpath('.//a/text()').extract_first()
                    id_in_href = utils.parse_id_from_uri(cell.xpath('.//a/@href').extract_first())
                    # print('check',headers[idx])
                    if headers[idx] not in check:
                        data[headers[idx]] = id_in_href
                        check.append(headers[idx])
                else:
                    # print('check', headers[idx])
                    if headers[idx] not in check:
                        data[headers[idx]] = utils.parse_num(cell.xpath('.//text()').extract_first())
                        check.append(headers[idx])
            all_data.append(data)
        return all_data