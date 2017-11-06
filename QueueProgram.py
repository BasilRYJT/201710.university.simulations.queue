# Written by Basil R. Yap
# Date: 2017/10/24 23:57

def exp_gen(mu):
    """generates a random variable that follows an exponential distribution with parameter mu"""
    from numpy.random import uniform
    from math import log
    return -log(uniform(0, 1)) / mu


def bern_gen(p):
    """"generates aa random variable that follows a bernoulli distribution with parameter p"""
    from numpy.random import uniform
    u = uniform(0, 1)
    if u < p:
        return True
    else:
        return False


def timemin_conv(time):
    """converts time into minutes"""
    return time * 60


# print("queuelist1: %s" % queuelist1)
# print("queuelist2: %s" % queuelist2)
# print("counter1: %s" % counter1)
# print("counter2: %s" % counter2)
# print("leavelist1: %s" % leavelist1)
# print("leavelist2: %s" % leavelist2)
# print("output: %s" % decision)
# print("waitlist: %s " % waitlist)
# print("totaltimelist: %s" % tottimelist)

def queue_decision(len1,
                   len2,
                   mu1,
                   mu2,
                   maxlen1,
                   maxlen2,
                   model="normal",
                   prob=0.5):
    """determines which queue customer joins"""
    if maxlen1 != -1 and len1 > maxlen1:
        raise ValueError("Queue 1 is packed like sardines, somebody is not supposed to be there.")
    if maxlen2 != -1 and len2 > maxlen2:
        raise ValueError("Queue 2 is packed like sardines, somebody is not supposed to be there.")
    if model == "random":  # random
        if prob > 1 or prob < 0:
            raise ValueError("probably impossible probability.")
        if len1 == maxlen1:
            if len2 == maxlen2:
                return "MAX"
            else:
                return "B"
        if len2 == maxlen2:
            return "A"
        elif bern_gen(prob):
            return "A"
        else:
            return "B"
    elif model == "normal":  # queue > service rate > random
        if len1 == maxlen1:
            if len2 == maxlen2:
                return "MAX"
            else:
                return "B"
        if len2 == maxlen2:
            return "A"
        if len1 < len2:
            return "A"
        elif len2 < len1:
            return "B"
        elif mu1 < mu2:
            return "B"
        elif mu2 < mu1:
            return "A"
        else:
            if prob > 1 or prob < 0:
                raise ValueError("probably impossible probability.")
            if bern_gen(prob):
                return "A"
            else:
                return "B"
    elif model == "workload":  # workload > random
        if len1 == maxlen1:
            if len2 == maxlen2:
                return "MAX"
            else:
                return "B"
        if len2 == maxlen2:
            return "A"
        wrkld1 = len1 * mu1
        wrkld2 = len2 * mu2
        if wrkld1 < wrkld2:
            return "A"
        elif wrkld2 < wrkld1:
            return "B"
        else:
            if prob > 1 or prob < 0:
                raise ValueError("probably impossible probability.")
            if bern_gen(prob):
                return "A"
            else:
                return "B"
    elif model == "queue":  # shortest queue > random
        if len1 == maxlen1:
            if len2 == maxlen2:
                return "MAX"
            else:
                return "B"
        if len2 == maxlen2:
            return "A"
        if len1 < len2:
            return "A"
        elif len2 < len1:
            return "B"
        else:
            if prob > 1 or prob < 0:
                raise ValueError("probably impossible probability.")
            if bern_gen(prob):
                return "A"
            else:
                return "B"
    else:
        raise ValueError("you haven't added this option yet.")


def counter_decision(lst1,
                     lst2,
                     jckyin=0.0,
                     model="FIFO"):
    """determines which customer in the queue is served next and what the new queue looks like"""
    from numpy.random import uniform
    from copy import deepcopy
    if jckyin < 0 or jckyin > 1:
        raise ValueError("probably impossible probability.")
    value = []
    queuelist2 = deepcopy(lst2)
    queuelist1 = deepcopy(lst1)
    if model == "FIFO":  # queue position
        if lst1:
            cust = queuelist1.pop(0)
        else:
            cust = "Na"
        value = [cust, queuelist1, queuelist2]
    elif model == "random":  # random
        if lst1:
            denom = 1 / len(queuelist1)
            cust = queuelist1.pop(int(uniform(0, 1) // denom))
        else:
            cust = "Na"
        value = [cust, queuelist1, queuelist2]
    elif model == "LIFO":  # reverse queue position
        if lst1:
            cust = queuelist1.pop()
        else:
            cust = "Na"
        value = [cust, queuelist1, queuelist2]

    if value:
        checkstp = abs(len(value[1]) - len(value[2]))
        if checkstp > 1 and jckyin > 0:
            if len(value[1]) < len(value[2]):
                checkpos = len(value[1])
                tar = deepcopy(value[1])
                src = deepcopy(value[2])
            else:
                checkpos = len(value[2])
                tar = deepcopy(value[2])
                src = deepcopy(value[1])
            while checkpos < (len(src) - 1):
                if bern_gen(jckyin):
                    tar.append(src.pop(checkpos))
                else:
                    checkpos += 1
            if len(value[1]) < len(value[2]):
                value[1] = tar
                value[2] = src
            else:
                value[2] = tar
                value[1] = src
        return value

    else:
        raise ValueError("you haven't added this option yet.")


def dep_decision(preemp=0.0,
                 preempdist=0.0,
                 icecrm=0.0,
                 fail=0.0):
    if preemp < 0 or preemp > 1:
        raise ValueError("probably impossible probability.")
    if icecrm < 0 or icecrm > 1:
        raise ValueError("probably impossible probability.")
    if fail < 0 or fail > 1:
        raise ValueError("probably impossible probability.")
    return "n"


def queue(start,  # start time
          end,  # end time
          servrate_1,  # service rate of queue 1
          servrate_2,  # service rate of queue 2
          maxlen1=-1,  # maximum length of queue 1
          maxlen2=-1,  # maximum length of queue 2
          maxwait=-1,  # maximum wait time of customer
          arrbhv="random",  # customer arrival behaviour
          qbhv1="FIFO",  # queue 1 service behaviour
          qbhv2="FIFO",  # queue 2 service behaviour
          parr=0.5,  # probability of picking queue 1 at random
          jckyin=0.0):  # probability of jockeying occurring for each customer
    """simulates queue from start time to end time with two service rates"""
    from scipy import mean
    from uuid import uuid4
    if end <= start:
        raise ValueError("the shop was not open for the day, everyone went home.")
    if start < 0 or end < 0:
        raise ValueError("time cannot be negative in value, genius.")
    if maxlen1 != -1 and maxlen1 <= 0:
        raise ValueError("there was a queue outside the shop, no-one thought of coming in.")
    if maxlen2 != -1 and maxlen2 <= 0:
        raise ValueError("there was a queue outside the shop, no-one thought of coming in.")
    if maxwait != -1 and maxwait <= 0:
        raise ValueError("the crowd mas very impatient and left because the counter wouldn't go to them.")
    maxint = (2 ** 63) - 1
    muarr = 1 / 5
    if maxwait == -1:
        waittime = maxint
    else:
        waittime = maxwait
    tnow = timemin_conv(start)  # current time
    tend = timemin_conv(end)  # closing time
    tarr = tnow + exp_gen(muarr)  # arrival time
    tdep1 = maxint  # departure time for first counter
    tdep2 = maxint  # departure time for second counter
    leavelist = {"max": maxint}  # list of departure time if customer waited too long for first counter
    n = 0  # number of customers in system
    bsinesshrs = True  # indicator if shop is still open
    queuelist1 = []  # list of customers in first queue
    queuelist2 = []  # list of customers in second queue
    counter1 = "Na"  # customer served by first counter
    counter2 = "Na"  # customer served by second counter
    startdict = {}  # log of entry time into the system
    ndict = {}  # log of number of customers in system
    waitlist = []  # list of time spent in queue
    tottimelist = []  # list of time spent in system
    # startdictcnt = {"counter1": tnow, "counter2": tnow}
    # idlelist1 = []  # list of idle time in system for counter 1
    # idlelist2 = []  # list of idle time in system for counter 2


    while bsinesshrs == True:
        if min([tarr, tdep1, tdep2, tend, min(leavelist.values())]) == tarr:  # arrival occurs
            tnow = tarr
            tarr += exp_gen(muarr)
            n += 1
            ndict[tnow] = n
            custid = uuid4()
            startdict[custid] = tnow
            decision = queue_decision(len(queuelist1), len(queuelist2), servrate_1, servrate_2, maxlen1, maxlen1,
                                      model=arrbhv, prob=parr)
            if decision == "A":  # customer chooses queue A
                tdep1 = tnow + exp_gen(servrate_1)
                if queuelist1 == [] and counter1 == "Na":
                    counter1 = custid
                    waitlist.append(0)
                else:
                    queuelist1.append(custid)
                    leavelist[custid] = tnow + waittime
            elif decision == "B":  # customer chooses queue B
                tdep2 = tnow + exp_gen(servrate_2)
                if queuelist2 == [] and counter2 == "Na":
                    counter2 = custid
                    waitlist.append(0)
                else:
                    queuelist2.append(custid)
                    leavelist[custid] = tnow + waittime
            elif decision == "MAX":  # no space in any queue
                tottimelist.append("Left")
                waitlist.append("Left")
                n -= 1
            else:  # invalid queue chosen
                raise ValueError("Customer made his/her own queue. wth you doing, customer.")
        if min([tarr, tdep1, tdep2, tend, min(leavelist.values())]) == tdep1:  # departure from queue 1 occurs
            tnow = tdep1
            n -= 1
            ndict[tnow] = n
            tottimelist.append(tnow - startdict[counter1])
            decision = counter_decision(queuelist1, queuelist2, model=qbhv1, jckyin=jckyin)
            if decision[0] != "Na":
                del leavelist[decision[0]]
            queuelist1 = decision[1]
            queuelist2 = decision[2]
            counter1 = decision[0]
            if queuelist1 and counter1 == "Na":
                counter1 = queuelist1.pop(0)
                waitlist.append(tnow - startdict[counter1])
            if queuelist2 and counter2 == "Na":
                counter2 = queuelist2.pop(0)
                tdep2 = tnow + exp_gen(servrate_2)
                waitlist.append(tnow - startdict[counter2])
            tdep1 = tnow + exp_gen(servrate_1)
            if queuelist1 == [] and counter1 == "Na":
                tdep1 = maxint
            else:
                waitlist.append(tnow - startdict[counter1])
        if min([tarr, tdep1, tdep2, tend, min(leavelist.values())]) == tdep2:  # departure from queue 2 occurs
            tnow = tdep2
            n -= 1
            ndict[tnow] = n
            tottimelist.append(tnow - startdict[counter2])
            decision = counter_decision(queuelist2, queuelist1, model=qbhv2, jckyin=jckyin)
            if decision[0] != "Na":
                del leavelist[decision[0]]
            queuelist2 = decision[1]
            queuelist1 = decision[2]
            counter2 = decision[0]
            if queuelist1 and counter1 == "Na":
                counter1 = queuelist1.pop(0)
                tdep1 = tnow + exp_gen(servrate_1)
                waitlist.append(tnow - startdict[counter1])
            if queuelist2 and counter2 == "Na":
                counter2 = queuelist2.pop(0)
                waitlist.append(tnow - startdict[counter2])
            tdep2 = tnow + exp_gen(servrate_2)
            if queuelist2 == [] and counter2 == "Na":
                tdep2 = maxint
            else:
                waitlist.append(tnow - startdict[counter2])
        if min([tarr, tdep1, tdep2, tend, min(leavelist.values())]) == tend:  # shop closes
            bsinesshrs = False
            tarr = maxint
        if min([tarr, tdep1, tdep2, tend, min(leavelist.values())]) == min(
                leavelist.values()):  # impatient customer from queue leaves
            tnow = min(leavelist.values())
            n -= 1
            ndict[tnow] = n
            custid = min(leavelist, key=leavelist.get)
            if custid in queuelist1:
                queuelist1.remove(custid)
            else:
                queuelist2.remove(custid)
            del leavelist[custid]
            tottimelist.append("Left")
            waitlist.append("Left")

    while n > 0:  # store closed and final customers are being cleared
        if min([tdep1, tdep2, min(leavelist.values())]) == tdep1:  # departure from queue 1 occurs
            tnow = tdep1
            n -= 1
            ndict[tnow] = n
            tottimelist.append(tnow - startdict[counter1])
            decision = counter_decision(queuelist1, queuelist2, model=qbhv1, jckyin=jckyin)
            if decision[0] != "Na":
                del leavelist[decision[0]]
            queuelist1 = decision[1]
            queuelist2 = decision[2]
            counter1 = decision[0]
            if queuelist1 and counter1 == "Na":
                counter1 = queuelist1.pop(0)
                waitlist.append(tnow - startdict[counter1])
            if queuelist2 and counter2 == "Na":
                counter2 = queuelist2.pop(0)
                tdep2 = tnow + exp_gen(servrate_2)
                waitlist.append(tnow - startdict[counter2])
            tdep1 = tnow + exp_gen(servrate_1)
            if queuelist1 == [] and counter1 == "Na":
                tdep1 = maxint
            else:
                waitlist.append(tnow - startdict[counter1])
        if min([tdep1, tdep2, min(leavelist.values())]) == tdep2 and n != 0:  # departure from queue 2 occurs
            tnow = tdep2
            n -= 1
            ndict[tnow] = n
            tottimelist.append(tnow - startdict[counter2])
            decision = counter_decision(queuelist2, queuelist1, model=qbhv2, jckyin=jckyin)
            if decision[0] != "Na":
                del leavelist[decision[0]]
            queuelist2 = decision[1]
            queuelist1 = decision[2]
            counter2 = decision[0]
            if queuelist1 and counter1 == "Na":
                counter1 = queuelist1.pop(0)
                tdep1 = tnow + exp_gen(servrate_1)
                waitlist.append(tnow - startdict[counter1])
            if queuelist2 and counter2 == "Na":
                counter2 = queuelist2.pop(0)
                waitlist.append(tnow - startdict[counter2])
            tdep2 = tnow + exp_gen(servrate_2)
            if queuelist2 == [] and counter2 == "Na":
                tdep2 = maxint
            else:
                waitlist.append(tnow - startdict[counter2])
        if min([tdep1, tdep2, min(leavelist.values())]) == min(
                leavelist.values()) and n != 0:  # impatient customer from queue 1 leaves
            tnow = min(leavelist.values())
            n -= 1
            ndict[tnow] = n
            custid = min(leavelist, key=leavelist.get)
            if custid in queuelist1:
                queuelist1.remove(custid)
            else:
                queuelist2.remove(custid)
            del leavelist[custid]
            tottimelist.append("Left")
            waitlist.append("Left")

    return [ndict, waitlist, tottimelist, mean([i for i in ndict.values()]), waitlist.count(0) / len(waitlist),
            waitlist.count("Left") / len(waitlist)]


# print(queue(9, 17, 0.15, 0.18))

def main(servlst1, servlst2, model, qbhv, n=10000, dir="D:\\Documents\\", ndict=False):
    """iterator to generate n number of queues"""
    from scipy import mean
    from copy import deepcopy
    from os import makedirs
    from os.path import exists
    print("### PROGRAM START ###")
    tmpdir = deepcopy(dir) + model + "_" + qbhv
    if not exists(tmpdir):
        makedirs(tmpdir)
    rawcsv = open(tmpdir + "\\0.0_rawstat.csv", "w")
    avgstatcsv = open(tmpdir + "\\0.0_avgstat.csv", "w")
    rawcsv.write("run, u1, u2, avgwait, avgtottime, avgn, avgempty, avgleft\n")
    avgstatcsv.write("u1, u2, avgwait, avgtottime, avgn, avgempty, avgleft\n")
    for srv1 in servlst1:
        for srv2 in servlst2:
            if ndict:
                ndictcsv = open(tmpdir + "\\%s_%s_ntime.csv" % (srv1, srv2), "w")
                ndictcsv.write("u1, u2, run, time, ncount\n")
            waitlst = []
            tottimelst = []
            nlst = []
            emplst = []
            lftlst = []
            for i in range(n):
                value = queue(9, 17, srv1, srv2, arrbhv=model, qbhv1=qbhv, qbhv2=qbhv)
                print("[%s, %s](%s) queue value: DONE" % (srv1, srv2, str(i + 1)))
                if ndict:
                    dict = value[0]
                    for key in sorted(dict.keys()):
                        row = "%s, %s, %s, %s, %s\n" % (srv1, srv2, i + 1, key, dict[key])
                        ndictcsv.write(row)
                    print("(%s) dictionary write: DONE" % str(i + 1))
                tmpwait = deepcopy(value[1])
                tmpwait = mean(list(filter(("Left").__ne__, tmpwait)))
                tmptot = deepcopy(value[2])
                tmptot = mean(list(filter(("Left").__ne__, tmptot)))
                waitlst.append(tmpwait)
                tottimelst.append(tmptot)
                nlst.append(value[3])
                emplst.append(value[4])
                lftlst.append(value[5])
                print("[%s, %s](%s) average write: DONE" % (srv1, srv2, str(i + 1)))
                row = "%s, %s, %s, %s, %s, %s, %s, %s\n" % (
                i + 1, srv1, srv2, tmpwait, tmptot, value[3], value[4], value[5])
                rawcsv.write(row)
            row = "%s, %s, %s, %s, %s, %s, %s\n" % (
                srv1, srv2, mean(waitlst), mean(tottimelst), mean(nlst), mean(emplst), mean(lftlst))
            avgstatcsv.write(row)

    print("### PROGRAM END ###")


u1 = [1/x for x in range(1, 11)]
u2 = [1/x for x in range(1, 11)]

lst1 = ["random", "queue", "workload"]
lst2 = ["FIFO"]
# lst2 = ["FIFO", "random", "LIFO"]

for j in lst2:
    for i in lst1:
        main(u1, u2, i, j)

# lst3 = [x / 10 for x in range(1, 6)]
#
# u11 = [1 / 9]
# u21 = [1]
# u12 = [1 / 7]
# u22 = [1 / 7]
#
# main(u11, u21, "random", "FIFO")
# main(u12, u22, "random", "FIFO")
#
# u11 = [1 / 7]
# u21 = [1 / 10]
# u12 = [1 / 8]
# u22 = [1 / 8]
#
# main(u11, u21, "queue", "FIFO")
# main(u12, u22, "queue", "FIFO")
#
# u11 = [1 / 7]
# u21 = [1 / 10]
# u12 = [1 / 8]
# u22 = [1 / 8]
#
# main(u11, u21, "workload", "FIFO")
# main(u12, u22, "workload", "FIFO")
