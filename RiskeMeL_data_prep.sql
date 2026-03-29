/* RISK EMEL */

-- Kredi Skoru 'Yüksek Riskli' olan(Risk Durumu Aktif olan müşterileri) Riskli müşterileri tahminleme sistemi oluşturacağım

/* 1.Öncelikle Querybank projesinden kullanacağım tabloları duplicate ediyorum */

-- Musteri, Hesap, Kredi, Alarm tablolarını direk aldım.
-- CR -> Credist Risk 

CREATE TABLE CR_CRM AS
SELECT * FROM Musteri;

SELECT * FROM CR_CRM;

CREATE TABLE CR_Account AS
SELECT * FROM Hesap;

SELECT * FROM CR_Account;

CREATE TABLE CR_Credit AS
SELECT * FROM Kredi;

SELECT * FROM CR_Credit;

CREATE TABLE CR_Risk AS
SELECT * FROM Alarm;

SELECT * FROM CR_Risk;


-- To Do List  !!!!

/*

TEMP TABLE oluşturma adımları

-- 1.CR_CRM, CR_Account, CR_Credit, CR_Risk tablolarını birleştirdik
     --Sadece hesabı olan müşterileri eşleştirdik, hesabı olmayan olanları dahil etmedik bu yüzden INNER JOIN ile birleştirdim CR_Accouunt ı, 
     --Ama diğer tablolar kredisi olmayan ya da riskli olmayan müşterileri de dahil edip null değer olmasını sağladım
-- 2.Sütunlarda düzenleme gerçekleştirdim (duplicate sütunları çıkardım, aynı isimli sütunların isimlerini değiştirdim)
   -- CR_CRM -> Soyad,TCNO,SubeId,KayitTarihi çıkardım, Durum -> Musteri_Durum olarak isimlendirdim
   -- CR_Account -> HesapNo,MusteriId,OlusturmaTarihi çıkardım, FaizOrani -> Hesap_Faiz_Orani, Durum -> Hesap_Durum olarak isimlendirdim
   -- CR_Credit -> KrediId,MusteriId çıkardım, FaizOrani -> Kredi_Faiz_Orani, Durum -> Kredi_Durum olarak isimlendirdim
   -- CR_Risk -> AlarmId,MusteriId,AlarmTarihi,Aciklama çıkardım,  Durum -> Risk_Durum olarak isimlendirdim
-- 3.Elimdeki değişkenlerden yeni sütun ürettim, feature türetme, sonra işime yaramayacak olanları dropladım
   -- CR_CRM -> Ad sütunundaki isimleri 'KADIN','ERKEK' olarak ayırıp Cinsiyet türettim, sonra Ad ı dropladım
                DogumTarihi sütunundan bugüne kadar geçen yılları hesaplayıp Yas türettim, sonra DogumTarihi ni dropladım
   -- CR_Credit -> Bugünden VadeTarihi ne kadar geçen süreyi gün olarak hesaplatıp Son_Odeme_KalanGun turettim, sonra VadeTarihi ni dropladım
                   BasvuruTarihi nden bugüne kadar geçen süreyi gün olarak hesaplatıp Ilk_Odeme_GecenGun turettim, sonra BasvuruTarihi ni dropladım 
-- 4.Veri setindeki NaN, None yazan formata aykırı değerleri null formatına çekmeyi göstermek istedim 
      Bunun için öncelikle temp table da CR_Credit tablosundan çektiğim, 
            -- Anapara,KalanBorc,FaizOrani 'NaN' stringi atadım COALESCE ile null olanlara, FaizOrani -> Kredi_Faiz_Orani olarak isimlendirdim
            -- Durum u 'None' stringi atadım COALESCE ile null olanlara, Durum -> Kredi_Durum olarak isimlendirdim
-- 5.Hesaplama adımı 
     Her müşteri için bir şube skoru oluşturmak istedim,bunun için öncelikle temp table da şunları oluşturdum
     -- Aynı şubedeki müşteriler arasında risk sırasını, Sube_Ici_Risk_Sira -> SubeId ye göre gruplayarak RiskSkor larını sıraladım
     -- Şubeler Arası Risk Dağılımı hesapladım, yani şubelerin risk yüzdelik sıralaması, Sube_Risk_Yuzde -> önce LATERAL JOIN de RiskSkoru_AVG hesapladım, bu ortalama RiskSkoru ile Sıralamanın Yüzdesini alarak son 2 basamağını round ettim
     Daha sonra esas tabloda bu değişkenler ile Sube_RiskSkoru hesaplayacağım.
     -- KalanBorc ve Anapara yı eğer sıfır değilse bölsün, sonra bu bölmenin kümülatif dağılımını hesaplayarak hangi yüzde diliminde olduğunu göstersin diye Borc_Yuku_Yuzde değişkeni türettim
-- 6.LATERAL JOIN adımı
     -- Önce join in içinde her müşteri için bulunduğu şubedeki ortalama risk skorunu hesaplıyoruz, RiskSkoru_AVG türettik
     -- Sonra, her müşteri satırına şube bazlı ortalama risk skorunu eklemek için lateral join kullanıyoruz
     Böylelikle subquery vs kullanmadan ekstra bir temp table yaratmadan ürettiğimiz RiskSkoru_AVG yi select sorgusunda kullanabiliyoruz

GERÇEK DATA PREP TABLOSUNU oluşturma adımları

-- 1.Yas sütununu bin lere ayırıp gruplayarak, 
     -- 18-29 yaş aralığını 'GENÇ', 
     -- 30-49 yaş aralığını 'ORTA YAŞLI', 
     -- 50 ve üstünü 'YAŞLI' olarak etiketledim ve Yas_Kategori olarak isimlendirdim
-- 2.Hesasplamış olduğum Sube_Risk_Yuzde yi 1/Sube_İci_Risk_Sira ile çarparak bir Sube_Risk_Skoru hesapladım
-- 3.Anapara,KalanBorc, Kredi_Faiz_Orani değişkenlerini 'NaN' değerleri, Kredi_Durum 'None' değerleri NULL yaptım
-- 4.SubeId, Sube_Ici_Risk_Sira, Sube_Risk_Yuzde değişkenlerini dropladım

*/

/* 2.Temp table oluşturuyoruz data prep için */

-- Önce tabloyu veri tiplerine göre create ediyorum
CREATE GLOBAL TEMPORARY TABLE CREDIT_RISK_ANALYTIC_DATA_PREP_TEMP
(
    MusteriId          NUMBER,
    Cinsiyet           VARCHAR2(20),
    Yas                NUMBER,
    RiskSkoru          NUMBER,
    Musteri_Durum      VARCHAR2(20),
    SubeId             NUMBER,
    Sube_Ici_Risk_Sira NUMBER,
    Sube_Risk_Yuzde    NUMBER,
    HesapTuru          VARCHAR2(20),
    Bakiye             NUMBER,
    Limit              NUMBER,
    Hesap_Faiz_Orani   NUMBER,
    Hesap_Durum        VARCHAR2(20),
    Anapara            VARCHAR2(50),
    KalanBorc          VARCHAR2(50),
    Borc_Yuku_Yuzde    NUMBER,
    Kredi_Faiz_Orani   VARCHAR2(50),
    Son_Odeme_KalanGun NUMBER,
    Kredi_Durum        VARCHAR2(20),
    Ilk_Odeme_GecenGun NUMBER,
    AlarmTipi          VARCHAR2(20),
    Risk_Durum         VARCHAR2(20)
) ON COMMIT PRESERVE ROWS;

-- Sonra içini verileri işleyerek insert ediyorum
INSERT INTO CREDIT_RISK_ANALYTIC_DATA_PREP_TEMP
SELECT c.MusteriId, 
       CASE 
           WHEN c.Ad IN ('Ayşe','Fatma','Cansu','Deniz','Ece','Selin','Zeynep','Seda','Elif','Gamze','Melis','Buse','Pelin',
                         'Şule','Sevim','İpek','Aslı','Nazlı','Hande','Mine','Tuba','Sibel','Nil','Özlem','Sevgi','Gül','Funda')
           THEN 'Kadın'
           WHEN c.Ad IN ('Ahmet','Mehmet','Ali','Veli','Can','Burak','Emre','Hakan','Murat','Gökhan','Serkan','Okan','Yusuf',
                         'Halil','Osman','Ferhat','Kadir','Onur','Barış','Tolga','Arda','Kerem','Levent')
           THEN 'Erkek'
           ELSE null
       END AS Cinsiyet,
       TRUNC(MONTHS_BETWEEN(SYSDATE, c.DogumTarihi) / 12) AS Yas, 
       c.RiskSkoru,
       c.Durum as Musteri_Durum,
       a.SubeId,
       RANK() OVER (PARTITION BY a.SubeId ORDER BY c.RiskSkoru DESC) AS Sube_Ici_Risk_Sira,
       ROUND(PERCENT_RANK() OVER (ORDER BY s.RiskSkoru_AVG DESC),2) AS Sube_Risk_Yuzde,
       a.HesapTuru,
       a.Bakiye,
       a.Limit,
       a.FaizOrani as Hesap_Faiz_Orani,
       a.Durum as Hesap_Durum,       
       COALESCE(TO_CHAR(cr.Anapara), 'NaN') AS Anapara,
       COALESCE(TO_CHAR(cr.KalanBorc), 'NaN') AS KalanBorc,
       ROUND(CUME_DIST() OVER (ORDER BY (cr.KalanBorc / NULLIF(cr.Anapara,0)) DESC),2) AS Borc_Yuku_Yuzde,
       COALESCE(TO_CHAR(cr.FaizOrani), 'NaN') AS Kredi_Faiz_Orani,
       TRUNC(TO_DATE(cr.VadeTarihi, 'DD/MM/YYYY') - SYSDATE ) AS Son_Odeme_KalanGun,
       COALESCE(cr.Durum, 'None') AS Kredi_Durum,
       TRUNC(SYSDATE - TO_DATE(cr.BasvuruTarihi, 'DD/MM/YYYY')) AS Ilk_Odeme_GecenGun,   
       r.AlarmTipi,
       r.Durum as Risk_Durum
FROM CR_CRM c
INNER JOIN CR_Account a
    ON c.MUSTERIID = a.MUSTERIID
LEFT JOIN CR_CREDIT cr
    ON c.MUSTERIID = cr.MUSTERIID
LEFT JOIN CR_RISK r
    ON c.MUSTERIID = r.MUSTERIID
JOIN LATERAL (
    SELECT AVG(c2.RiskSkoru) AS RiskSkoru_AVG
    FROM CR_CRM c2
    JOIN CR_Account a2 ON c2.MusteriId = a2.MusteriId
    WHERE a2.SubeId = a.SubeId
    GROUP BY a2.SubeId
) s ON 1=1;

SELECT * FROM CREDIT_RISK_ANALYTIC_DATA_PREP_TEMP;

/* 3.Python a gidecek esas data prep tablosunu oluşturuyorum */

-- Önce tabloyu veri tiplerine göre create ediyorum
CREATE TABLE CREDIT_RISK_ANALYTIC_DATA_PREP (
    MusteriId          NUMBER,
    Cinsiyet           VARCHAR2(20),
    Yas                NUMBER,
    Yas_Kategori       VARCHAR2(20),
    RiskSkoru          NUMBER,
    Musteri_Durum      VARCHAR2(20),
    Sube_Risk_Skoru    NUMBER,
    HesapTuru          VARCHAR2(20),
    Bakiye             NUMBER,
    Limit              NUMBER,
    Hesap_Faiz_Orani   NUMBER,
    Hesap_Durum        VARCHAR2(20),
    Anapara            VARCHAR2(50),
    KalanBorc          VARCHAR2(50),
    Kredi_Faiz_Orani   VARCHAR2(50),
    Son_Odeme_KalanGun NUMBER,
    Kredi_Durum        VARCHAR2(20),
    Borc_Yuku_Yuzde    NUMBER,
    Ilk_Odeme_GecenGun NUMBER,
    AlarmTipi          VARCHAR2(20),
    Risk_Durum         VARCHAR2(20)
);

-- Sonra içini verileri işleyerek insert ediyorum
INSERT INTO CREDIT_RISK_ANALYTIC_DATA_PREP 
SELECT t.MusteriId,
       t.Cinsiyet,
       t.Yas,
       CASE 
           WHEN t.Yas BETWEEN 18 AND 29 THEN 'Genç'
           WHEN t.Yas BETWEEN 30 AND 49 THEN 'Orta Yaşlı'
           WHEN t.Yas >= 50 THEN 'Yaşlı'
       END AS Yas_Kategori,
       t.RiskSkoru,
       t.Musteri_Durum,
       ROUND(t.Sube_Risk_Yuzde*(1/t.Sube_Ici_Risk_Sira),2) AS Sube_Risk_Skoru,
       t.HesapTuru,
       t.Bakiye,
       t.Limit,
       t.Hesap_Faiz_Orani,
       t.Hesap_Durum,
       CASE 
           WHEN t.Anapara = 'NaN' THEN NULL 
           ELSE t.Anapara 
       END AS Anapara,
       CASE 
           WHEN t.KalanBorc = 'NaN' THEN NULL 
           ELSE t.KalanBorc 
       END AS KalanBorc,
       CASE 
           WHEN t.Kredi_Faiz_Orani = 'NaN' THEN NULL 
           ELSE t.Kredi_Faiz_Orani 
       END AS Kredi_Faiz_Orani,       
       t.Son_Odeme_KalanGun,
       CASE 
           WHEN t.Kredi_Durum = 'None' THEN NULL 
           ELSE t.Kredi_Durum 
       END AS Kredi_Durum,
       t.Borc_Yuku_Yuzde,
       t.Ilk_Odeme_GecenGun,
       t.AlarmTipi,
       t.Risk_Durum
FROM CREDIT_RISK_ANALYTIC_DATA_PREP_TEMP t;

SELECT * FROM CREDIT_RISK_ANALYTIC_DATA_PREP;

-- DROP TABLE CREDIT_RISK_ANALYTIC_DATA_PREP;


/*
Neden Global Temp Table (GTT) Kullandım ?
- Ara sonuçları bir kez hesaplayıp tabloya yazıyoruz, böylelikle her çalıştırmada tüm tabloyu yeniden hesaplamıyor, büyük veri için ideal
- Sonraki adımlarda bu tabloyu tekrar kullanabiliyoruz,performans artıyor.
- Incremental insert yapılabilir, sadece yeni satırlar eklenir.
- Session bazlı olduğu için oturum kapandığında içerik temizlenir,disk yükü azalır.

- CREATE GLOBAL TEMPORARY TABLE temp_table_name → Geçici tablo oluşturuyoruz, adı "temp_table_name".
- ON COMMIT PRESERVE ROWS → Transaction commit edildiğinde satırlar silinmez, oturum boyunca kalır.
*/

-- (python) encode edilecekler -> Musteri_Durum, Hesap_Durum, Kredi_Durum, Risk_Durum ,Cinsiyet label
-- (python) HesapTuru, AlarmTipi,yas_kategori encode et one hot
-- (python) kardinal veri droplanacak sütunlar algoritmaya girerken -> MusteriId
-- sonra missing value ları doldur