---
title: Blog posts
layout: default
nav_exclude: true
nav_show: false
permalink: /blog/
---
{% for post in site.posts %}
* [{{ post.title }}]({{ post.url | absolute_url }})

  _{{ post.date | date: "%B %d, %Y" }} by {{ post.author }}_
  {: .text-small .lh-0  .text-grey-dk-000 }
  {{ post.description }}
  {: .fs-3 }
{% endfor %}
