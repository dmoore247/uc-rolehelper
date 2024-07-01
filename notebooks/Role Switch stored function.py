# Databricks notebook source
def trigger(name):
    import requests
    return requests.get('https://gist.githubusercontent.com/dmoore247/3fd4bd8377a42d429f0e0b975db783c1/raw/19372835bc2eb0a68e4d1609332af226251b781c/gist.json').text

return trigger(s) if s else None
