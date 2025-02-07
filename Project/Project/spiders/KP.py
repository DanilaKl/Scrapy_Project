from collections.abc import Iterable

import scrapy
from parsel import Selector
from playwright.async_api import Page
from scrapy import Request
from scrapy.http import Response

from ..items import ArticleItem


def should_abort_request(request):
    return ("yandex" in request.url or
            "ya" in request.url or
            "google" in request.url or
            "smi2" in request.url)


class KpSpider(scrapy.Spider):
    name = "KP"
    allowed_domains = ["kp.ru"]
    required_articles_count = 1000
    total_scanned_articles = 0

    custom_settings = {
        "PLAYWRIGHT_ABORT_REQUEST": should_abort_request,
        "PLAYWRIGHT_LAUNCH_OPTIONS": {"headless": False},
        "DOWNLOAD_HANDLERS": {
            "http": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
            "https": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
        },
        "ITEM_PIPELINES": {
            'Project.pipelines.PhotoDownloaderPipeline': 101,
            'Project.pipelines.MongoPipeline': 303,
        },
        "MONGO_URI": "mongodb://admin:admin@localhost:27017",
        "MONGO_DATABASE": "admin",
    }

    def start_requests(self) -> Iterable[Request]:
        yield scrapy.Request(
            url="https://www.kp.ru/online/",
            meta={"playwright": True, "playwright_include_page": True},
        )

    async def parse(self, response: Response):
        page: Page = response.meta["playwright_page"]
        page_number = 1
        while self.total_scanned_articles < self.required_articles_count:
            page_selector = Selector(await page.content())
            article_hrefs = page_selector.xpath(
                f"(//section)[{page_number}]//a[contains(@class, 'drlShK')]/@href"
            )
            self.logger.info(f"{page_number}: {len(article_hrefs)}")
            for article_href in article_hrefs:
                yield scrapy.Request(url=response.urljoin(str(article_href)),
                                     callback=self.parse_article)
            await page.locator(selector="//button[@class='sc-abxysl-0 cdgmSL']").click(position={"x": 176, "y": 26.5})
            await page.wait_for_timeout(10000)
            self.total_scanned_articles += len(article_hrefs)
            page_number += 1
            print(self.total_scanned_articles)
        await page.close()

    def parse_article(self, response: Response):
        title = response.xpath("//h1/text()").get()
        description = response.xpath("//div[contains(@class, 'nFVxV')]/text()").get()
        article_text = response.xpath(
            "(//div[@data-gtm-el='content-body'])[1]"
            "//p[contains(@class, 'dqbiXu')]/text()"
        ).getall()
        publication_datetime = response.xpath(
            "(//div[contains(@class, 'dQphFo')])[1]/span/text()"
        ).get()
        header_photo_url = response.xpath(
            "//img[contains(@class, 'cYprnQ')]/@src"
        ).get()
        keywords = response.xpath(
            "(//div[contains(@class, 'dQphFo')])[1]/a/text()"
        ).getall()
        authors = response.xpath(
            "//div[contains(@class, 'gcyEOm')]"
            "//span[contains(@class, 'bmkpOs')]/text()"
        ).getall()
        source_url = response.url

        if (
            title is None or description is None or
            not article_text or publication_datetime is None or
            not keywords or not authors or source_url is None
        ):
            return

        return ArticleItem(title=title.strip(),
                           description=description.strip(),
                           article_text="".join(article_text),
                           publication_datetime=publication_datetime,
                           header_photo_url=header_photo_url,
                           keywords=keywords,
                           authors=authors,
                           source_url=source_url)
