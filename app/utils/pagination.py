from sqlalchemy.orm import Query


def paginate_query(
    query: Query,
    limit: int = 50,
    offset: int = 0,
) -> dict:
    """
    Apply pagination to any SQLAlchemy query.
    Returns items plus pagination metadata.
    """
    limit = min(max(1, limit), 500)
    offset = max(0, offset)
    total = query.count()
    items = query.offset(offset).limit(limit).all()
    return {
        "items": items,
        "total": total,
        "limit": limit,
        "offset": offset,
        "has_more": offset + limit < total,
        "next_offset": offset + limit if offset + limit < total else None,
    }
