def _parse_url(url):
    if url.endswith("/"):
        return url[:-1]
    return url