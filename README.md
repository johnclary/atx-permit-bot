# atx-permit-bot
*This is a personal project and has no affiliation with or endorsement from the City of Austin.*

This bot tweets about new permits issued in Austin, TX. It does so by scraping the City of Austin's [Build & Connect Portal](https://abc.austintexas.gov/).

The bot tweets these permit types:

- Residential and commercial building permits\*
- Zoning/Rezoning
- Film
- Short Term Rental (all types)
- Street Vendor
- Hotel
- Easement Release

\* Excluding commercial and residential remodels and rennovations.

## How It Works

The bot is written in Python and is comprised of three components:

1. The scraper (`scrape.py`) retrieves new permit data and posts it to the tweet server.

2. The tweet server (`tweet-server.py`) is a [Sanic](https://github.com/huge-success/sanic) app which listens for permit data and tweets it.

3. A [PostgREST](http://postgrest.org/)-fronted PostgreSQL database archives permit data and keeps track of what's been tweeted.