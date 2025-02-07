from http import HTTPStatus
from os import getenv
from typing import Annotated, Any, Mapping

from fastapi import FastAPI, Depends, HTTPException
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, Field
import pymongo
from pymongo.asynchronous.collection import AsyncCollection


class NewsArticle(BaseModel):
    title: str
    description: str
    article_text: str
    publication_datetime: str
    header_photo_url: str | None = Field(None)
    header_photo_base64: str | None = Field(None)
    keywords: list[str] = Field([])
    authors: list[str] = Field([])
    source_url: str


app = FastAPI(title="News Page Generator service", description="Study Case Example")


async def get_mongo_db() -> AsyncCollection[Mapping[str, Any] | Any]:
    mongo_user = getenv("MONGO_USER", "admin")
    mongo_password = getenv("MONGO_PASSWORD", "admin")
    mongo_port = getenv("MONGO_PORT", 27017)
    mongo_uri = f"mongodb://{mongo_user}:{mongo_password}@localhost:{mongo_port}/"
    mongo_db = getenv("MONGO_DATABASE", "admin")
    mongo_db_collection = getenv("MONGO_DATABASE_COLLECTION", "scraped_articles")
    client = pymongo.AsyncMongoClient(mongo_uri)
    return client[mongo_db][mongo_db_collection]


@app.get("/articles", tags=["HTML Article Manager"])
async def get_random_articles_in_html(
    mongo_db: Annotated[AsyncCollection[Mapping[str, Any] | Any], Depends(get_mongo_db)],
    size: int = 10,
) -> HTMLResponse:
    results = await mongo_db.aggregate([{"$sample": {"size": size}}])
    if not results:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND,
            detail="There is no any articles",
        )
    articles = [NewsArticle(**result) async for result in results]
    html_content = """
        <html>
        <head>
            <meta charset="utf-8"/>
            <title>Новости онлайн</title>
            <style>
                body {
                    font-family: Arial, sans-serif;
                    margin: 20px;
                }
                .article {
                    border: 1px solid #ccc;
                    margin-bottom: 20px;
                    padding: 10px;
                }
                .title {
                    font-size: 18px;
                    font-weight: bold;
                }
                .description {
                    font-weight: 600;
                    color: #555;
                }
                .article_text {
                    margin: 10px 0;
                }
                .datetime, .keywords, .authors, .source_url {
                    margin: 5px 0;
                    color: #444;
                }
                .source_url a {
                    color: #0066cc;
                    text-decoration: none;
                }
                .source_url a:hover {
                    text-decoration: underline;
                }
                img {
                    max-width: 300px;
                    display: block;
                    margin-top: 10px;
                }
            </style>
        </head>
        <body>
            <h1>Сводка новостей</h1>
        """
    for article in articles:
        html_content += f"""
            <div class="article">
                <div class="title">{article.title}</div>
                <div class="description">{article.description}</div>
                <div class="article_text">{article.article_text}</div>
                <div class="datetime"><b>Дата публикации:</b> {article.publication_datetime}</div>
                <div class="keywords"><b>Ключевые слова:</b> {", ".join(article.keywords)}</div>
                <div class="authors"><b>Авторы:</b> {", ".join(article.authors)}</div>
                <div class="source_url"><b>Ссылка на источник:</b> 
                    <a href="{article.source_url}" target="_blank">{article.source_url}</a>
                </div>
            """
        if article.header_photo_base64:
            html_content += f"""
                <img src="data:image/jpeg;base64,{article.header_photo_base64}"alt="header photo"/>
                <div class="source_url"><a href="{article.header_photo_url}">Ссылка на фото</a></div>
                """
        html_content += "</div>"

    html_content += """
        </body>
        </html>
        """

    return HTMLResponse(content=html_content, status_code=200)
