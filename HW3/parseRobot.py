__author__ = 'snehagaikwad'

import robotparser

def ParseRobot(robotFile,url):
    rp = robotparser.RobotFileParser()
    rp.set_url(robotFile)
    rp.read()
    canFetch = rp.can_fetch("*", url)
    return canFetch

if __name__ == '__main__':
    robotFile = "http://en.wikipedia.org/robots.txt"
    url1 = "http://en.wikipedia.org/wiki/Doron_Sheffer"
    url2 = "http://en.wikipedia.org/wiki/Category:Basketball_players_from_Illinois"
    canFetch1 = ParseRobot(robotFile,url1)
    print url1, canFetch1
    canFetch2 = ParseRobot(robotFile,url2)
    print url2, canFetch2