from utils.user_agent import get_random_ua

# Redis配置
REDIS_CONFIG = {
    "host": "localhost",
    "port": 6379,
    "db": 2,
    "password": "1234",
    "queue_name": "taobao_links"
}

# 签名配置（保留你的加密参数）
SIGN_CONFIG = {
    "appkey": "12574478",
    "js_path": "utils/et加密.js",  # 与你的JS文件路径一致
    "token_cookie_key": "_m_h5_tk"
}

# MySQL配置
MYSQL_CONFIG = {
    "host": "localhost",
    "port": 3306,
    "user": "spider",
    "password": "1234",
    "database": "TB_Data",
    "charset": "utf8mb4"
}

# 爬虫配置
CRAWL_CONFIG = {
    "max_retry": 3,
    "timeout": 10,
    "thread_num": 5,
    "total_page": 6,  # 分页总数

    'detail_page': 10,
    "headers": {
        'cookie':'thw=cn; wk_cookie2=150db39d5a3a2d519c2ac5768007f6df; cookie2=1283ab0499b2d692c03c051b6bee00c8; t=f847df6161a417f87fb0263dcefeff77; _tb_token_=f71e7a568a867; xlly_s=1; mt=ci=0_0; cna=3EMVIFrtFG0CATryJoTEeuZb; wk_unb=UUpgT7xo1ddfT5zb4A%3D%3D; lgc=tb948373071913; cancelledSubSites=empty; dnk=tb948373071913; tracknick=tb948373071913; _hvn_lgc_=0; havana_lgc2_0=eyJoaWQiOjIyMTkzNzY3NjExNTIsInNnIjoiYTUyNjkzODE3NWJjZGRlMWEzMTM4ZWMwMzNkOGFkNGQiLCJzaXRlIjowLCJ0b2tlbiI6IjFwRVNvT1dmS0s5UkxEb213UkZhR3B3In0; cnaui=2219376761152; aui=2219376761152; 3PcFlag=1755854684774; cookie3_bak=1283ab0499b2d692c03c051b6bee00c8; cookie3_bak_exp=1756113885955; unb=2219376761152; env_bak=FM%2BgytvMDCSuqt5UTFnnD922Ydw8ckYs2dl4%2BIkuYc3R; cookie17=UUpgT7xo1ddfT5zb4A%3D%3D; _l_g_=Ug%3D%3D; sg=324; _nk_=tb948373071913; cookie1=UU22yY0iVQM78EqEuKNggvLd4jdSwxBVh3drFFAdMJk%3D; sn=; sca=30d90b68; _samesite_flag_=true; havana_sdkSilent=1756039578112; uc1=pas=0&cookie14=UoYbzWKsG%2BGBIw%3D%3D&cookie16=UIHiLt3xCS3yM2h4eKHS9lpEOw%3D%3D&cookie15=VFC%2FuZ9ayeYq2g%3D%3D&existShop=false&cookie21=W5iHLLyFe3xm; uc3=id2=UUpgT7xo1ddfT5zb4A%3D%3D&vt3=F8dD2fcaFZV3cRmmlOs%3D&lg2=V32FPkk%2Fw0dUvg%3D%3D&nk2=F5RMHUyCmMkzk9C%2FBMU%3D; csg=06f1775f; skt=6a079298742afe80; existShop=MTc1NjAxMTA0Ng%3D%3D; uc4=id4=0%40U2gqwAO23NmKb%2BSpKemEA%2FMNFXccc34h&nk4=0%40FY4HWGY1PVK7wdnwXPScZxGMmdQQrTeIag%3D%3D; _cc_=Vq8l%2BKCLiw%3D%3D; sgcookie=E100MN3dEGoomI9IvzGFoGp1nbFkmMSIkDVXTXnYhRFZdfua%2BOMIWvJMh%2Fo2bCZsPfTfqIZcc6wIzNmX5kBdCF5I3g2okHPHgfHZpVXTaWHlr583nrqBRrarXfKS1SzcPmbe; havana_lgc_exp=1787115046009; sdkSilent=1756039846009; mtop_partitioned_detect=1; _m_h5_tk=369e73690d3032174ee70d8ba06028fd_1756034752625; _m_h5_tk_enc=43db491c4c6485c464be11b4d6001323; tfstk=grVxIaAAhaLYLdx2klXkSMVAxRbl-T4VmozBscmDCuE850AGujXZ6ln8bmDmGmAt6PEajlUNuVi_flngnTf3urlZ1MVOt6427IvG_Sh6jzGqSxSlr3f3ur8Wkh_3F6YOpdss5di_fbiS72G6GIM1ybgKVIg6GVt5yVoS1qOj5_MSuqtXcKZ_Pa3ZVcGshlG5yVoSffGsQEB-bKiH6-B37aPGCmR6173xhT4jVCgzwqHxArNv1CTZkxnQl0sEDx9tBoHYZsKEk8aLOqENTIljBRNmNSs51baUEog8fMptVJF46vVAvpobgcr-K-IpMkEq5uh0ONWqHyg_Rvwf7egzhoFSB7bNUxwUmJM8ZidrA8a8SYeP7KmgLVZZL5jegma_8Wyn9sKEk8aKwg-YtWdQdHmKSK_RydJZh4oKhZktIw1RP4nhrqvwQYBryDbRAdJZh4u-xaj9QdkRU; isg=BCYmi-zzchu_GymbFeQ0uOV3d5qoB2rBJHWYjxDPEskmk8ateJe60Qxl648fO2LZ',
            "user-agent": get_random_ua()

    },
    'detail_headers': {
        'cookie': 'cna=3EMVIFrtFG0CATryJoTEeuZb; wk_cookie2=150db39d5a3a2d519c2ac5768007f6df; xlly_s=1; lid=tb948373071913; wk_unb=UUpgT7xo1ddfT5zb4A%3D%3D; cookie3_bak=1283ab0499b2d692c03c051b6bee00c8; env_bak=FM%2BgytvMDCSuqt5UTFnnD922Ydw8ckYs2dl4%2BIkuYc3R; cookie3_bak_exp=1756113885955; isg=BA8PUleOu67a5rB0RKVGp5sQnqUZNGNW5bZh4CEcq36F8C_yKQTzpg3z8CDOiDvO; _l_g_=Ug%3D%3D; lgc=tb948373071913; cookie1=UU22yY0iVQM78EqEuKNggvLd4jdSwxBVh3drFFAdMJk%3D; login=true; cookie2=1283ab0499b2d692c03c051b6bee00c8; cancelledSubSites=empty; sg=324; sn=; _tb_token_=f71e7a568a867; dnk=tb948373071913; tracknick=tb948373071913; unb=2219376761152; cookie17=UUpgT7xo1ddfT5zb4A%3D%3D; _nk_=tb948373071913; t=f847df6161a417f87fb0263dcefeff77; havana_sdkSilent=1756039846009; uc1=pas=0&cookie14=UoYbzWKsG%2BGBIw%3D%3D&cookie16=UIHiLt3xCS3yM2h4eKHS9lpEOw%3D%3D&cookie15=VFC%2FuZ9ayeYq2g%3D%3D&existShop=false&cookie21=W5iHLLyFe3xm; uc3=id2=UUpgT7xo1ddfT5zb4A%3D%3D&vt3=F8dD2fcaFZV3cRmmlOs%3D&lg2=V32FPkk%2Fw0dUvg%3D%3D&nk2=F5RMHUyCmMkzk9C%2FBMU%3D; uc4=id4=0%40U2gqwAO23NmKb%2BSpKemEA%2FMNFXccc34h&nk4=0%40FY4HWGY1PVK7wdnwXPScZxGMmdQQrTeIag%3D%3D; havana_lgc_exp=1787115046009; sgcookie=E100jXtR1RDfBXt04SjysUvelRmFnPeRoFPixOmG%2FpokxsMYhunZxwlILESc5FUZjx3fEaCAB3xnwq76hbvBzK7uikcQuj8f8%2BCLY3js5HCDwVJlwrc7kr%2BVJ6xXduS0c2G3; csg=06f1775f; mtop_partitioned_detect=1; _m_h5_tk=71f1e18d7dcc6e76a61261d776d43b7c_1756032735751; _m_h5_tk_enc=4c0759784773f4328a542ecb4ac0f563; x5sectag=428416; x5sec=7b2274223a313735363032363731382c22733b32223a2262656434313665646230613531396534222c22617365727665723b33223a22307c434e697571385547454d694d36726e362f2f2f2f2f774561447a49794d546b7a4e7a59334e6a45784e5449374d69494a5932467763485636656d786c4d4c375270717747227d; tfstk=gLF-QhORFZLJy6N-ozWD-G2ojG7mpt4rMuz6tDmkdoEYbldhz8q3H2Z0vXc3RgrL9mZJt84uRe3QmbMUEDmovXUUXZjGs14zzXl1jGfMytpMsXoCtX6mG6ahAnsGs145F4l3lGV3Ch2mm2GIP0MBGZ3ZypGIF01fk2gHVpO7AZQx-4nBABg7ljgrRXiQAX_YlmuKO2ZIOAllHDJSjBeRIkg21_qXOBNxyY60PmOIuS3-eca7wBOCU4H-fzifb0MDkvE-eSfJb4ebW5g8Y1RrNveQZjN5c6Gb7-qSkks9pmwTaWHgwGpZV7oUDA2RXQET42ZxZ5_FUYUsRPM_Fw8su8Z8GvFc8tk7Qu2sHSjJgcwbkyDY3wJoxRuQYAPhbQo7RPPaIfCJb4ebWjIzh5V91RtiX2v5kZpeLYgqXY_rpb4mc00xjZ0kLpkA3qnGkG9eLYgqkcbcvpJEHt5..',




            "user-agent": get_random_ua()

    }
}
