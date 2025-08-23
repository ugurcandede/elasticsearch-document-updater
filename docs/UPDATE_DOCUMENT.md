# Update Document Operations - Detaylı Kılavuz

Bu operasyon Elasticsearch dokümanlarındaki organizasyon bilgisini günceller.

## Ne Yapar?

Bu operasyon dokümanlardaki organizasyon alanını günceller:

```javascript
ctx._source.organization = organizationId
```

## Kullanım

```bash
python main.py --operation update_document --env local --tenant_id develop --data result.json
```

**Not**: Bu operasyon için `--tenant_id` parametresi zorunludur!

## Veri Formatı

```json
[
  [1, [6]],
  [3, [61, 91, 103, 118]],
  [4, [78, 92]],
  [7, [76]],
  [8, [67, 75, 100, 110]],
  [11, [69]],
  [12, [28]],
  [13, [68, 87, 111]],
  [14, [96]],
  [21, [86, 93, 102]],
  [22, [82]],
  [23, [14, 55]],
  [27, [9]],
  [29, [58]],
  [32, [18]]
]
```

- **Key**: Organization ID
- **Value**: Array of requester ID'leri

## result.json Dosyasının Oluşturulması

### SQL Sorgusu

```sql
SELECT json_agg(json_build_array(organization_id, user_ids))
FROM (SELECT au.organization_id, jsonb_agg(DISTINCT au.id) AS user_ids
      FROM ticket t
               LEFT JOIN app_user au ON t.requester_id = au.id
      WHERE au.organization_id IS NOT NULL
      GROUP BY au.organization_id
      ) subquery;
```

### Veritabanı Tabloları

Bu operasyon şu tablolardan veri çeker:
- `ticket`: Ticket bilgileri
- `app_user`: Kullanıcı bilgileri

### Filtreleme Kriterleri

- `au.organization_id IS NOT NULL`: Organization ID'si olan kullanıcılar
- `GROUP BY au.organization_id`: Organization'a göre gruplama
- `jsonb_agg(DISTINCT au.id)`: Tekrarsız kullanıcı ID listesi

## İşlem Mantığı

Her organization için:
1. Organization ID'yi al
2. Bu organization'a ait requester ID'leri al
3. Her requester ID için ayrı güncelleme isteği gönder
4. İlgili dokümanların organization alanını güncelle

## Hedef Dokümanlar

Güncelleme şu kriterlere uyan dokümanları etkiler:
```javascript
fieldMap.ts.requester.value = requesterId
```

## Örnek Sonuç

```json
{
  "100": ["req_001", "req_002", "req_003"],
  "200": ["req_101", "req_102"],
  "300": ["req_201", "req_202", "req_203", "req_204"]
}
```

Bu durumda:
- Organization 100: 3 requester için dokümanlar güncellenecek
- Organization 200: 2 requester için dokümanlar güncellenecek  
- Organization 300: 4 requester için dokümanlar güncellenecek

## Tenant ID Gereksinimi

Bu operasyon diğerlerinden farklı olarak tek bir tenant üzerinde çalışır:
- `--tenant_id` parametresi zorunludur
- Elasticsearch URL'si: `{env}/{tenant_id}_tickets_{environment}/_update_by_query`

## Paralel İşlem

Her requester ID için ayrı thread oluşturulur, bu nedenle büyük organizasyonlar için çok sayıda paralel istek gönderilir.
