from elasticsearch import BadRequestError, Elasticsearch, NotFoundError
from fastapi import FastAPI, HTTPException, Query

app = FastAPI(title="Pipeline Big Data API")

es = Elasticsearch("http://localhost:9200")


@app.get("/")
def root():
    return {"message": "API FastAPI OK"}


@app.get("/health")
def health():
    return {"elasticsearch_ok": es.ping()}


@app.get("/indices")
def list_indices():
    try:
        res = es.cat.indices(format="json")
        return res.body if hasattr(res, "body") else res
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e


@app.get("/search")
def search_jobs(
    q: str = Query(..., description="mot-clé à rechercher"),
    index: str = Query("job_skill_matches", description="nom de l'index"),
    size: int = Query(10, ge=1, le=100),
):
    body = {
        "query": {
            "multi_match": {
                "query": q,
                "fields": ["title^3", "description", "company", "location", "skills"],
                "fuzziness": "AUTO",
            }
        },
        "size": size,
    }

    try:
        res = es.search(index=index, body=body)
        data = res.body if hasattr(res, "body") else res

        return {
            "total": data["hits"]["total"],
            "hits": [
                {"id": h["_id"], "score": h["_score"], "source": h["_source"]}
                for h in data["hits"]["hits"]
            ],
        }

    except NotFoundError as e:
        raise HTTPException(status_code=404, detail=f"L'index '{index}' n'existe pas.") from e

    except BadRequestError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e
