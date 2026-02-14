import requests

cookies = {
    'BNES_ASP.NET_SessionId': '25Rp4LoPJ3UiqGHJf9d///t7o9wnw+Ab8K3T+wSqLzKZNjpYUQdE1CXgOwT4ms7nc0+EdWuEB6rzlzmHj0Z2FRp4nlPDhmXp',
    'cf_clearance': 'WGWXK5vOo9wU_PJ0R7s7IQEj9loUrMC6KAv4XNBQF0k-1770103737-1.2.1.1-kcSkvO5F8AB.KREErb4OBMP6EdqrJniwVIbQCYz2zDo7dXSyaT.md0ofPv7cddLzFmXITVOmNbstw4vY_YGlGNnJ829P4FfVY.Y3a6Ky.zI7N_Fya2Jn7TdxaC7EreFkYUBhnigvsc964fsZqZLzLSR8hu6ICk58FBxKKu9g3TvQQ_5V_Nc0fnzgE4JBksZi22sq8Z7clxqMn3gp_pZnGGaBsar6_MLHfpeHzpkvvJk',
}

headers = {
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36',
}

params = {
    'id': '8115',
}

response = requests.get(
    'https://s1.sos.mo.gov/Records/Archives/ArchivesMvc/Naturalization/Detail',
    params=params,
    cookies=cookies,
    headers=headers,
)
print(response.text)