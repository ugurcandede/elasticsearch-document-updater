# ElasticSearch Bulk Document Updater - Unified Version

Bu proje, üç farklı Elasticsearch bulk güncelleme işlemini tek bir modüler yapı altında birleştirir.

## Özellikler

Bu unified script üç farklı Elasticsearch operasyonunu destekler:

1. **Add Field Operations** - Tüm dokümanlara yeni alan ekleme
2. **Field Type Operations** - Doküman alanlarının tipini güncelleme  
3. **Document Operations** - Dokümanlardaki organizasyon bilgisini güncelleme

## Proje Yapısı

```
elasticsearch-bulk-updater/
├── main.py                 # Ana script dosyası
├── requirements.txt        # Gerekli Python paketleri
├── README.md              # Bu dosya
├── config.ini.example     # Yapılandırma şablonu
├── docs/                  # Dokümantasyon ve örnek dosyalar
│   ├── ADD_FIELD.md       # Alan ekleme detaylı kılavuzu
│   ├── FIELD_TYPE.md      # Alan tipi güncelleme detaylı kılavuzu
│   ├── UPDATE_DOCUMENT.md # Doküman güncelleme detaylı kılavuzu
│   ├── sample_add_field.json         # Add field örnek veri
│   ├── sample_field_type.json        # Field type örnek veri
│   └── sample_document_update.json   # Document update örnek veri
├── src/                   # Modüler kod yapısı
│   ├── __init__.py
│   ├── config.py          # Konfigürasyon ayarları
│   ├── utils.py           # Ortak yardımcı fonksiyonlar
│   ├── add_field_operations.py      # Alan ekleme operasyonları
│   ├── field_type_operations.py     # Alan tipi güncelleme operasyonları
│   └── document_operations.py       # Doküman güncelleme operasyonları
├── add-a-field-to-all-docs/         # Orijinal proje (sadece okuma)
├── field_type_updater/              # Orijinal proje (sadece okuma)
└── update_document/                 # Orijinal proje (sadece okuma)
```

## Kurulum

1. Gerekli Python paketlerini yükleyin:
```bash
pip install -r requirements.txt
```

## Kullanım

### Komut Satırı Parametreleri

- `--operation`: Yapılacak işlem türü (add_field, field_type, update_document)
- `--env`: Hedef environment (local, dev, net, prod)
- `--data`: JSON veri dosyası yolu (varsayılan: result.json)
- `--tenant_id`: Tenant ID (update_document işlemi için gerekli)
- `--max_workers`: Maksimum thread sayısı (varsayılan: 4)

### 1. Alan Ekleme İşlemi (Add Field)

Tüm dokümanlara yeni alan ekler (`fieldMap["ts.scope"]`).

```bash
python main.py --operation add_field --env local --data result.json
```

**Veri formatı:**
```json
{
  "tenant1": 37,
  "tenant2": 487
}
```

➡️ **[Detaylı kılavuz ve SQL sorguları için tıklayın](docs/ADD_FIELD.md)**

### 2. Alan Tipi Güncelleme İşlemi (Field Type)

Belirli alanların tipini `NUMBER_DECIMAL`'e günceller.

```bash
python main.py --operation field_type --env local --data result.json
```

**Veri formatı:**
```json
{
  "tenantId1": [
    {"field_key1": ["ticket_key1", "ticket_key2"]},
    {"field_key2": ["ticket_key3", "ticket_key4"]}
  ]
}
```

➡️ **[Detaylı kılavuz ve SQL sorguları için tıklayın](docs/FIELD_TYPE.md)**

### 3. Doküman Güncelleme İşlemi (Update Document)

Dokümanlardaki organizasyon bilgisini günceller.

```bash
python main.py --operation update_document --env local --tenant_id develop --data result.json
```

**Veri formatı:**
```json
{
  "organizationId1": ["requesterId1", "requesterId2"],
  "organizationId2": ["requesterId3", "requesterId4"]
}
```

➡️ **[Detaylı kılavuz ve SQL sorguları için tıklayın](docs/UPDATE_DOCUMENT.md)**

## Environment Ayarları

Desteklenen environment'lar:
- `local`: Development environment
- `dev`: Staging environment  
- `net`: Pre-production environment
- `prod`: Production environment

## Örnekler

### IDE'de Çalıştırma

Script'i IDE'de çalıştırmak için `main.py` dosyasını açıp parametreleri kod içinde ayarlayabilir veya run configuration'da belirtebilirsiniz.

### Toplu İşlem

Farklı operasyonları sırayla çalıştırmak için:

```bash
# Önce alan ekle
python main.py --operation add_field --env local --data docs/sample_add_field.json

# Sonra alan tipini güncelle
python main.py --operation field_type --env local --data docs/sample_field_type.json

# Son olarak dokümanları güncelle
python main.py --operation update_document --env local --tenant_id develop --data docs/sample_document_update.json
```

## Hata Yönetimi

- Script her operasyon için detaylı success/error logları üretir
- Hatalı JSON formatı durumunda açıklayıcı hata mesajları verir
- Network hataları için retry mekanizması bulunur

## Thread Sayısı Optimizasyonu

Varsayılan olarak 4 thread kullanılır. Elasticsearch sunucunuzun kapasitesine göre bu sayıyı artırabilirsiniz:

```bash
python main.py --operation add_field --env local --max_workers 8
```

## Güvenlik

- Production environment için extra dikkat gösterilmelidir
- Büyük veri setleri için önce test environment'da deneme yapılması önerilir
- Operation öncesi backup almanız önerilir

## Lisans

Bu proje MIT lisansı altında lisanslanmıştır.
