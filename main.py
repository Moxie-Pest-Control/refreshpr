import json
import logging
import os
import urllib.parse
from datetime import datetime, timedelta
from multiprocessing import Process

import pandas
import pyodbc
import requests
from dateutil.relativedelta import relativedelta
from pytz import timezone

import loadPestroutesData as loadData
import updatedData as updatedData

from variables import table


def main():
    print("Running ETL Process")
    date = datetime.strftime(datetime.now(),'%Y-%m-%d 00:00:00')
    date2 = datetime.strftime(datetime.now(),'%Y-%m-%d %H:%M:%S')
    date = datetime.strptime(str(date),'%Y-%m-%d %H:%M:%S') - relativedelta(months=2)
    date2 = datetime.strptime(str(date2),'%Y-%m-%d %H:%M:%S')

    endpoints = ["Payment","ServiceType","Subscription","Lead","Ticket","Note","PaymentProfile","Route","Task","PaymentApplication","Team","Knock","Spot","Changelog"]
    endpoints = ["Appointment","Subscription"]

    kvBranches = {
        "Ft Worth": {
            "officeID": 10,
            "authenticationKey": "8f77d8a19b535512f1147039f97c93cb",
            "authenticationToken": "cd1ff2c9d9792c04dc54adbd2d0eed56",
            "timeZone": "America/Chicago"
        },
        "Denver": {
            "officeID": 11,
            "authenticationKey": "c9ab804b19393610d46616ca9489e369",
            "authenticationToken": "6dce29da04e027624fc20b8687294b32",
            "timeZone": "America/Denver"
        },
        "Chantilly": {
            "officeID": 219,
            "authenticationKey": "3c234cc2d3db790413e14f2a54632955",
            "authenticationToken": "f7219ab02e456440d2f22e7aa644b4a7",
            "timeZone": "America/New_York"
        },
        "Las Vegas": {
            "officeID": 604,
            "authenticationKey": "BB5FF2A06B83469D0AD09FFA833359C6C4B017BC6A4239C51C625617EADCCC32",
            "authenticationToken": "DACEE1EB055DCBFE5B8B0DD09943B14C351F06014DEB758B45C793BA4B195733",
            "timeZone": "America/Los_Angeles"
        },
        "Stafford": {
            "officeID": 606,
            "authenticationKey": "E0C80CC6F7498240C85D2248138899297B27083CC0AD7B82F9AD80E83A621F4E",
            "authenticationToken": "031694B012661640BC75150CB870A17DAEBAAC17A715474AB95A98F4A900DF5B",
            "timeZone": "America/New_York"
        },
        "San Antonio": {
            "officeID": 712,
            "authenticationKey": "33f6f3d7f2a85be80b26fc7730614fa4c5f7492520eaf084a1c832e767362ad3",
            "authenticationToken": "e7ae20b35aac8ff37763e885368bafb59a64bc8ab715c73b872b72718a2df747",
            "timeZone": "America/Chicago"
        },
        "Atlanta": {
            "officeID": 732,
            "authenticationKey": "gekkmhceh6gd5nu5j12374isi178rala5p54qs7rb0d2hnmsn01lnkj192ed1ipi",
            "authenticationToken": "5m9rf04eq3nghb2790cvj4vb7ue1fncuqhhnfqfn8158fuan4umtr6re6jv273i2",
            "timeZone": "America/New_York"
        },
        "Maryland": {
            "officeID": 733,
            "authenticationKey": "5p7nuadp07hht95bjd2n07ksepk449q5183cvje20tn7ukrbssd3nisvgup528fa",
            "authenticationToken": "3auv1ek0aqu0qorggof07c9tb7h7ftrdk48m1u4gidkmdgrn62hcjur5erf3bgmo",
            "timeZone": "America/New_York"
        },
        "Austin": {
            "officeID": 734,
            "authenticationKey": "9avf81k4ugcmjbe452vg3eq7gusbhm6omb2o91n835kf9i1a3bsg6frbml1q0im7",
            "authenticationToken": "cjju1814ausmonma0kknv2ef1bjkdfv3idvjgejhbeicl6pu1egi9p2dsv99mrb3",
            "timeZone": "America/Chicago"
        },
        "Alexandria": {
            "officeID": 735,
            "authenticationKey": "97799fbff09e49b99fa6d60bd7e2238ed144a1d5d9d3525036afe2047131ee7e",
            "authenticationToken": "807e2057cc555d36b42afc3f7ef7646f3718cf07a4b6bae135276991b2a0b9ac",
            "timeZone": "America/New_York"
        },
        "Dallas": {
            "officeID": 736,
            "authenticationKey": "a6ab6f0e8f517914113932171d6f6afb35a059387af1447634340a11aa5624a7",
            "authenticationToken": "8d716c44daba364a3a2fcd1fdada5f752833ba4c74b76c5b2d6e57c366829ec8",
            "timeZone": "America/Chicago"
        },
        "West Palm Beach": {
            "authenticationKey": "jtigqvgb8vriunmlkc0m30qsvvta5bmd51fk9r1ug1oo5vfslelptbhgkadanc3n",
            "authenticationToken": "pqrbvcbg8gpak0jh7l9qk7t3jgn2q0ggt00vspqocvc8vl6skjn2p4srbhf1s7ka",
            "officeID": 738,
            "timeZone": "America/New_York"
        },
        "Kansas City": {
            "officeID": 15,
            "authenticationKey": "35fe27f60a654ed9e130064cad97a613",
            "authenticationToken": "39bf056783eda955e2e10118230a69de",
            "timeZone": "America/Chicago"
        },
        "Orange County": {
            "officeID": 117,
            "authenticationKey": "73d7e822f486e4d3c4c0b6ddd3567667",
            "authenticationToken": "6d67e5ef5e4199cd627d896a4aa10e13",
            "timeZone": "America/Los_Angeles"
        },
        "Raleigh": {
            "officeID": 321,
            "authenticationKey": "2669d47b47f728e89c42cf78d71f1a2c",
            "authenticationToken": "639501f6fddc7f0201fcfe3929555a63",
            "timeZone": "America/New_York"
        },
        "Utah": {
            "officeID": 423,
            "authenticationKey": "fecd6ec93e9de35cecba605744d57f6c",
            "authenticationToken": "cc3ddb06d7765102a21de30755bf5f3f",
            "timeZone": "America/Denver"
        },
        "Arizona": {
            "officeID": 525,
            "authenticationKey": "8604e20a4144a013c1bc8e21fbd68caf",
            "authenticationToken": "dc2cb9f980af8b9c50910806a60652a0",
            "timeZone": "America/Phoenix"
        },
        "Joshuas Pest Control": {
            "officeID": 526,
            "authenticationKey": "a69d21d14a6b20beb7f082134a7fe5c6",
            "authenticationToken": "59c0e97d0b32460ad17ce2a1c3f22ab6",
            "timeZone": "America/Los_Angeles"
        },
        "Moxie Nashville": {
            "officeID": 600,
            "authenticationKey": "4dad9c8ef1b14ae6b833712a997456b5",
            "authenticationToken": "a42e0fa04bbca239063c9297def52c5a",
            "timeZone": "America/Chicago"
        },
        "Moxie Oklahoma City": {
            "officeID": 601,
            "authenticationKey": "2fb516def9004dd79a9b7d8a024e8d80",
            "authenticationToken": "90612a8af68950034988430e5ab427c4",
            "timeZone": "America/Chicago"
        },
        "Moxie Columbus": {
            "officeID": 603,
            "authenticationKey": "EB5CF24B7F10252AD7A71411043762082E100AD1FCA7161324F8DD6ACC60CED0",
            "authenticationToken": "71CD3779B277E4BD1B42232BE62A7F68437CE1AA89E999D4D9B2530238235BCA",
            "timeZone": "America/New_York"
        },
        "Moxie Tucson": {
            "officeID": 605,
            "authenticationKey": "BAF0076B3157F5112293E82F7D461BF55D463B92D35B3462F65666D6EA15E809",
            "authenticationToken": "1A6FEE6BA2C16D2624EAF1359F809EB2BA61176233132AF58E5D935B6D3DEB95",
            "timeZone": "America/Phoenix"
        },
        "Joshua's Pest Control": {
            "officeID": 709,
            "authenticationKey": "65AACE0C8AD49EC1A98F3F3281845C42E08E64B424A4B09CDF323BB879EC8F42",
            "authenticationToken": "E5636E14880582A7D04330EAFFEA14E4883833F52B627FA2889613F9A9F67C4F",
            "timeZone": "America/Los_Angeles"
        },
        "Moxie St. George": {
            "officeID": 713,
            "authenticationKey": "552bf987c16a6c2fe19f2a2e0762daa24225b5c8b4a74a258231a756d4adc053",
            "authenticationToken": "b0d7c23bf1f718a551459bd77166192bcd141f6ddb090c605bb4d4fa0f858c96",
            "timeZone": "America/Denver"
        },
        "Mission Pest Control": {
            "officeID": 714,
            "authenticationKey": "928ed0a403b7abd80f69aa38b4ca534917fecce986362639c7977ea410ff5ad8",
            "authenticationToken": "a58cb26a8a5f277d63f95e6a0ffa9a145135cf2bf2fc645facdf463805047c47",
            "timeZone": "America/Los_Angeles"
        },
        "Moxie Riverside": {
            "officeID": 715,
            "authenticationKey": "a995fb1df8c7f2a4816ae6f22e259624c86ebf31f3b50d5a25789947b3fd47a4",
            "authenticationToken": "bc442da2e00bb72d1bed5e7418e526f9e469ef2dcdf2ae3f82d0f4aea5a78e28",
            "timeZone": "America/Los_Angeles"
        },
        "St. Louis": {
            "officeID": 718,
            "authenticationKey": "89901ca77934cb9a652d7a5f6dc1459ecb886bffd85fbdab0e6e7c707056a355",
            "authenticationToken": "faa09b19a087ed450fa9370ffafe2356ea3ca359c8c58111fb1ca8e04553bc40",
            "timeZone": "America/Chicago"
        },
        "Moxie Cincinnati": {
            "officeID": 719,
            "authenticationKey": "0b572db9ed2af0c577873e4d68089a4952ec9576de1fd03d1a5e6f641dfe1559",
            "authenticationToken": "f86fdb2cb4b7cdf471d126c44f97cf061f5fe7cbf34af6b7366951f5bb6ca673",
            "timeZone": "America/New_York"
        },
        "Minneapolis": {
            "officeID": 725,
            "authenticationKey": "bfaa10640a8ffee13b97bd762c7e630f86065fa00c0ea2ab7424a7ecde3cf44e",
            "authenticationToken": "1d8684fd8a2ae151ee445f73f9e7b935d4cfc3bebb112d5c7870b714dbf2b778",
            "timeZone": "America/Chicago"
        },
        "Chicago": {
            "officeID": 726,
            "authenticationKey": "1383a6e4441adafbf654cee4aa95471483c1e0535e45adddbf778dc76f8639ff",
            "authenticationToken": "2e2e6ea55c24faf48ad1ec9c009095d43691aa9b8e9fc7c71634954603438fe4",
            "timeZone": "America/Chicago"
        },
        "Moxie Tulsa": {
            "officeID": 729,
            "authenticationKey": "rae5kcrtm895p4o07vqs5hrgj5jr4dg06hvtiufh26u7tilacv16viufj4usmrtm",
            "authenticationToken": "q40v0ec1ghpgslg721l1qn7fs0p1d130lpcohgnmd8a9k9ceqem53cfr30f67scg",
            "timeZone": "America/Chicago"
        },
        "Moxie Pennsylvania": {
            "officeID": 731,
            "authenticationKey": "p3ntgsiqo27fv6fpaed2nrtti01e2hjqot691lg2utip0r3oh7f8uc571a5fsk5o",
            "authenticationToken": "3o3ahc9ionh1t57ka620k6dt41ge0a0l8spc7k2eaf0jj9uk3ugqgpdvvtbo3sur",
            "timeZone": "America/New_York"
        },
        "Indianapolis": {
            "officeID": 737,
            "authenticationKey": "ntfs3lhk4ap73ja4sdgqarbfpl0f3h7q9n6mkmj35nf2pmla6p6juvau0sjam034",
            "authenticationToken": "s5lme0sk4u1leu4ldn688oqpam8r5vqo200p0pbc69b13qlorblqdm698s27djbh",
            "timeZone": "America/New_York"
        },
        "Moxie Charlotte": {
            "officeID": 741,
            "authenticationKey": "f82hg7isvu2eoa25a3nloh2iiteclavbsdegj4lllsu5p2qbdo2m4gejr4a62j5k",
            "authenticationToken": "mn9h30k5sj27r210aurs039kh415n7cgd967uneif5jt0k22tcd8p9m0k1mu0tg0",
            "timeZone": "America/New_York"
        },
        "Green Monster": {
            "officeID": 742,
            "authenticationKey": "523imffpfin5jv0en261g5el1nfvasg06qmg5ml62l18869ug2qhfp1eph9hdhn4",
            "authenticationToken": "5nrcqtcbo65afil22dghm0bl813asjmk8r0lns2b5595i5m737jmg9pq2iav3i5v",
            "timeZone": "America/Los_Angeles"
        },
        "Cleveland": {
            "officeID": 751,
            "authenticationKey": "si1mns5fjafumafjupsub6b5ch02gvj3q2gag2kq9jnl8vsuo2pn07k8knpcn15h",
            "authenticationToken": "n1thfl086b6otc7qo6eu7hfb5etfbl9evvd1pvb35m59ohsrugahskefkkqdtpkk",
            "timeZone": "America/New_York"
        },
        "Bentonville": {
            "officeID": 750,
            "authenticationKey": "e5tfc5m5u9vu7hfo0vu0h1sjtfu6cuuikt8d60r2b9hftrob254791vn9dojhrbt",
            "authenticationToken": "nva7u1pb7ii062d1kjejs7mi7h1p7bq4f9bid1e55v35os10s262nrbb78i7f5s3",
            "timeZone": "America/Chicago"
        },
        "Pittsburgh": {
            "officeID": 747,
            "authenticationKey": "7lufa6m437qtnq451p3vjgj6m7b8lehacobomv8u25m16b1vqti6meu5bbv98jve",
            "authenticationToken": "qneumk67q4o808itgt995iqp9tlgihcodj1o2oi2vsf6f8rurdko4a59bof7k0fr",
            "timeZone": "America/New_York"
        },
        "Detroit": {
            "officeID": 752,
            "authenticationKey": "b97efpm43c9faeq4mm5bkic3a84hhkdnt6a3hh77nfhddm26njafaduqafr5cupj",
            "authenticationToken": "obm9cgupb5dnnd3rr5mfvjk140gd2dnq272riposf7lj06eq2c76rmr1icqh53hv",
            "timeZone": "America/Chicago"
        },
        "Louisville": {
            "officeID": 749,
            "authenticationKey": "jh34o82grc6lgnlrprened4kh6jg1c6ch55qoeaodlk8t1htdp4chb7jve27971p",
            "authenticationToken": "o7jc7rlb146vheskujibroggqspva064bt1r9pdeeqjk5dl145p5vtl25j9e5b61",
            "timeZone": "America/New_York"
        },
        "Boston": {
            "officeID": 746,
            "authenticationKey": "oa0lo7k2j8p7v9lhpcn5m26jertqn8ehcj026g5k62dcigim6lt0si5pp9iljjq8",
            "authenticationToken": "npru06g75rl7rgvrmsgs2rkpnhlas5t02k3jvbce00m09afb8risg04d4o7d36e1",
            "timeZone": "America/New_York"
        },
        "Houston": {
            "officeID": 748,
            "authenticationKey": "hmkj4588btm70qriv97llrjftkdu6ngr1fhq0vfcre5of85iue3e39tvek11f0sd",
            "authenticationToken": "3m9oocmrgffopkm47js20622se5d7jj9atc2vur3vpudv11rlfj6d3h2052ee7ii",
            "timeZone": "America/Chicago"
        },
        "Portland": {
            "officeID": 754,
            "authenticationKey": "ilcd71ho7d982ng9j1auntg0h5lj5v81jbqicnbsja866d83mqqr5o458etankh8",
            "authenticationToken": "75if8m6gtcraeg356mii7acvbtmb1mjsone1nfdvqos6v38jtfulgdidff36j22g",
            "timeZone": "America/Los_Angeles"
        },
        "Seattle": {
            "officeID": 756,
            "authenticationKey": "4qtrhc2fq3vvkatsm6odql76d476j5nn5k6sk3ntud31485d2fd5u7let7rb11kj",
            "authenticationToken": "50bm7q7huvp19mb9uqje31998orvjt1cpriigalm736qolm9g98c3fqpf6bpvm9v",
            "timeZone": "America/Los_Angeles"
        },
        "Norfolk": {
            "officeID": 757,
            "authenticationKey": "5ftqc4vjbrg8pacf0n0s0ajo47q7mjole1gehvcc7m1n9pvhnl4acbrnj5qbf5p9",
            "authenticationToken": "cfnl7al6a2588p6bmde5rndnsq81r7ni3a9782p5oo62slovfe1n1oiikl1pa49t",
            "timeZone": "America/New_York"
        },
        "Milwaukee": {
            "officeID": 753,
            "authenticationKey": "coj95setfe8802ohncfsc297umlj6id1gne4duhbkvahup7kqnq9r3osmpu8vdk7",
            "authenticationToken": "6ff5i5h4kjv39o3brv2q02rr1o628g6ru5gd3q4qil4od3ve3r2rliv4genrkjm5",
            "timeZone": "America/Chicago"
        },
        "Hartford": {
            "officeID": 745,
            "authenticationKey": "rtlllg3ggb45ggfin229ko1u9lb7e0ge6hb0bgvn25ja2rukkf3m20uodfo7c3rp",
            "authenticationToken": "thcf21lgbcnb0sh9om477epi92j2ng1obt1ri6uhum6e9vi5460cd0qaqbkl6jai",
            "timeZone": "America/New_York"
        },
        "Moxie Commercial": {
            "officeID": 758,
            "authenticationKey": "bc2h1i2c15s8iqr92s4lmc5pd0deho6dprk45os9d0s44dqdbobpjk837301he3v",
            "authenticationToken": "agf2dnvk0ibrqidimkv4dcharqnjed321uh5n3essa5bvc19mfb4vbqr08hl8bet",
            "timeZone": "America/Chicago"
        },
        "Tampa": {
            "officeID": 755,
            "authenticationKey": "uc984t0jn61d99jb2t0gbgnf7nguduvs93mbs217d8k26382ok55b38rt4o6vvgi",
            "authenticationToken": "h5s3o08opupn1q3plu09ju57ofsbdl2r4laidptnavioduccbdlooij5qoclirqk",
            "timeZone": "America/New_York"
        }
    }
    curDate = datetime.strftime(date2,'%Y-%m-%d')
    difference = ( pandas.to_datetime('2015-01-01') - pandas.to_datetime(curDate) )
    diffSec = difference.total_seconds()//3600
    while diffSec < 0:
        dtobj = datetime.strptime(str(date), '%Y-%m-%d %H:%M:%S')
        dtobj2 = datetime.strptime(str(date2), '%Y-%m-%d %H:%M:%S')
        dateFormat = datetime.strftime(dtobj,'%Y-%m-%d %H:%M:%S')
        dateFormat2 = datetime.strftime(dtobj2,'%Y-%m-%d %H:%M:%S')
        dateEncoded = urllib.parse.quote('{"operator":"BETWEEN","value":["'+dateFormat+'","'+dateFormat2+'"]}')
        print(' '+str(dateFormat)+' '+str(dateFormat2)+'  '+str(diffSec)+'   ')
        # print(dateEncoded)
        for item in endpoints:
            endpointData = table.tables[item]
            
            # with open('table.txt', 'w') as f:
            #     f.write(str(item))

            for branch in kvBranches:
                loadData.getIds(kvBranches[branch],endpointData,dateEncoded,item)
                updatedData.getIds(kvBranches[branch],endpointData,dateEncoded,item)
                # changelogUpdate.getIds(kvBranches[branch],dateEncoded)


        date = datetime.strptime(str(date),'%Y-%m-%d %H:%M:%S') - relativedelta(months=2)
        date2 = datetime.strptime(str(date2),'%Y-%m-%d %H:%M:%S') - relativedelta(months=2)
        with open('date1.txt', 'w') as f:
            f.write(str(date))
        with open('date2.txt', 'w') as f:
            f.write(str(date2))
        difference = ( pandas.to_datetime('2025-01-01') - pandas.to_datetime(date2) )
        diffSec = difference.total_seconds()//3600
        
main()
