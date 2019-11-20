# atx-permit-bot
*This is a personal project with no affiliation with or endorsement from the City of Austin.*

This bot tweets about new permits issued in Austin, TX. It does so by scraping the City of Austin's [Build & Connect Portal](https://abc.austintexas.gov/).

The bot tweets these permit types:

- Residential and commercial building permits\*
- Zoning/Rezoning
- Film
- Short Term Rental (all types)
- Street Vendor
- Hotel
- Easement Release

\* Excluding commercial and residential remodels and renovations.

## How It Works

The bot is written in Python and is comprised of three components:

1. A [PostgREST](http://postgrest.org/)-fronted PostgreSQL database stores permit data and keeps track of what's been tweeted.

2. `scrape.py` retrieves new permit data and loads it to the database.

3. `tweet.py` intermittently queries the permit database for tweetworthy permits, and tweets them.
