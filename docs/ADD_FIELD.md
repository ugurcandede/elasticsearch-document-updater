# Add Field Operations - Detaylı Kılavuz

Bu operasyon tüm Elasticsearch dokümanlarına yeni alan ekler.

## Ne Yapar?

Bu operasyon fieldMap'e yeni bir alan ekler:

```javascript
fieldMap["ts.scope"] = {"id": 123, "value": 20, "type": "SELECT", "order": 1, "name": "TICKET"}
```

## Kullanım

```bash
python main.py --operation add_field --env local --data result.json --field_key ts.scope
```

**Parametreler:**
- `--field_key`: Eklenecek alan anahtarı (varsayılan: ts.scope)

## Veri Formatı

```json
{
  "tenant1": 37,
  "tenant2": 487,
  "tenant3": 125
}
```

- **Key**: Tenant ID
- **Value**: Field option ID (fdeo.id)

## result.json Dosyasının Oluşturulması

### Adım 1: SQL Sorgu Oluşturma

**Önemli:** SQL sorgusunda `fd.key = 'YOUR_FIELD_KEY'` kısmını kendi field key'inizle değiştirin.

```sql
SELECT 'SELECT jsonb_object_agg(foo.tenantid, foo.json) FROM(' || string_agg(
        '(select ''' || mt.schema_name || ''' as tenantId, fdeo.id as json from ' || mt.schema_name || '.field_definition fd LEFT JOIN ' || mt.schema_name ||
        '.field_definition_entity_options fdeo on fd.id = fdeo.field_definition_entity_id WHERE fd.key = ''YOUR_FIELD_KEY'' AND fdeo.label = ''TICKET'')', ' UNION ') || ')as foo' || ';'
from main.tenant mt;
```
> Bu sorgu, her tenant için `ts.scope` alanına sahip ve `TICKET` option ID'lerini içeren bir SQL sorgu string'i oluşturur.

**Örnekler:**
- `ts.scope` field'ı için: `WHERE fd.key = 'ts.scope'`
- `ts.priority` field'ı için: `WHERE fd.key = 'ts.priority'`  
- `custom.status` field'ı için: `WHERE fd.key = 'custom.status'`

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
- `field_definition_entity_options`: Alan seçenekleri
- `main.tenant`: Tenant şema bilgileri

## Filtreleme Kriterleri

- `fd.key = 'ts.scope'`: Sadece ts.scope alanları
- `fdeo.label = 'TICKET'`: Sadece TICKET etiketli seçenekler

## Örnek Sonuç

```json
{
  "company_a": 34,
  "company_b": 56,
  "company_c": 78
}
```

Bu durumda:
- `company_a` tenant'ı için option ID 34 kullanılacak
- `company_b` tenant'ı için option ID 56 kullanılacak
- `company_c` tenant'ı için option ID 78 kullanılacak
