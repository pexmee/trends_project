# trends_project
This is a utility to be used by developers with access to the Twitter Developer Portal.

It is used for listening to new tweets containing a keyword and pattern. \
The utility will send all new tweets matching a keyword and pattern to an Elasticsearch instance,
and will keep all existing entries updated with the current count of retweets and likes. 

It does not store the actual tweet content, nor the name of the author or profile. 
