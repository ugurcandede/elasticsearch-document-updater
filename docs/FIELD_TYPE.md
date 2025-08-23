# Field Type Operations - Detaylı Kılavuz

Bu operasyon Elasticsearch dokümanlarındaki belirli alanların tipini günceller.

## Ne Yapar?

Bu operasyon fieldMap'teki alanların tipini günceller:

```javascript
fieldMap.<field_key>.type = 'NUMBER_DECIMAL'
```

## Kullanım

```bash
python main.py --operation field_type --env local --data result.json
```

## Veri Formatı

```json
{
  "tenantId1": [
    {"field_key1": ["ticket_key1", "ticket_key2"]},
    {"field_key2": ["ticket_key3", "ticket_key4"]}
  ],
  "tenantId2": [
    {"custom_field_amount": ["ticket_key5", "ticket_key6"]}
  ],
  "tenantId3": null
}
```

- **Key**: Tenant ID
- **Value**: Array of objects containing field_key -> ticket_keys mapping
- **null**: Bu tenant için işlem yapılmayacak

## result.json Dosyasının Oluşturulması

### Adım 1: SQL Sorgu Oluşturma

**Önemli:** SQL sorgusunda `fd.type = 'YOUR_TYPE'` kısmını kendi field key'inizle değiştirin.

Tüm tenant'ların schema isimlerini almak için bu SQL komutunu çalıştırın:

```sql
SELECT 'SELECT jsonb_object_agg(foo.tenantid, foo.json) FROM(' || string_agg('select ''' || mt.schema_name ||
                                                                             ''' as tenantId, jsonb_agg(json_build_object(fcja.field_key, fcja.ticket_keys)) as json from (select jsonb_agg(json_build_object(fd.key, cf.ticket_key)), fd.key AS field_key, jsonb_agg(cf.ticket_key) AS ticket_keys FROM ' ||
                                                                             mt.schema_name || '.field_definition fd LEFT JOIN ' || mt.schema_name ||
                                                                             '.custom_field cf ON fd.id = cf.definition_id WHERE fd.type = ''YOUR_TYPE'' AND cf.ticket_key IS NOT NULL GROUP BY fd.key) fcja',
                                                                             ' UNION ') || ') as foo' || ';'
from main.tenant mt
```

### Adım 2: Sorguyu Çalıştırma

Oluşturulan SQL sorgu string'ini kopyalayıp `\gexec` komutu ile psql'de çalıştırın:

> PostgreSQL Docker containerına bağlanma:
>```bash
>docker exec -it <container_id> psql -U <username> -d <database_name>
>```

```bash
SELECT 'SELECT jsonb_object_agg(foo ................. from main.tenant mt \gexec
```

### Adım 3: Sonucu Kaydetme

Sonucu kopyalayıp `result.json` dosyasına yapıştırın.

## Veritabanı Tabloları

Bu operasyon şu tablolardan veri çeker:
- `field_definition`: Alan tanımları
- `custom_field`: Custom field değerleri
- `main.tenant`: Tenant şema bilgileri

## Filtreleme Kriterleri

- `fd.type = 'NUMBER_DECIMAL'`: Sadece NUMBER_DECIMAL tipindeki alanlar
- `cf.ticket_key IS NOT NULL`: Ticket key'i olan kayıtlar
- `GROUP BY fd.key`: Field key'e göre gruplama

## İşlem Mantığı

Her tenant için:
1. Field definition'dan NUMBER_DECIMAL tipindeki alanları bul
2. Bu alanlarla ilişkili custom field'ları bul  
3. Her field_key için ticket_key listesini oluştur
4. Her ticket_key için ayrı güncelleme isteği gönder

## Örnek Sonuç

```json
{
  "company_a": [
    {"amount_field": ["TICKET-1", "TICKET-2"]},
    {"price_field": ["TICKET-3"]}
  ],
  "company_b": [
    {"cost_field": ["TICKET-100", "TICKET-101", "TICKET-102"]}
  ]
}
```

Bu durumda:
- `company_a` tenant'ında:
  - `amount_field` alanı TICKET-001 ve TICKET-002 için NUMBER_DECIMAL yapılacak
  - `price_field` alanı TICKET-003 için NUMBER_DECIMAL yapılacak
- `company_b` tenant'ında:
  - `cost_field` alanı üç farklı ticket için NUMBER_DECIMAL yapılacak
