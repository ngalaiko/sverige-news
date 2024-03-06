use crate::feeds;
use crate::id::Id;
use crate::persisted::Persisted;

pub static FEED: once_cell::sync::Lazy<Persisted<feeds::Feed>> = once_cell::sync::Lazy::new(|| {
    let created_at = chrono::DateTime::parse_from_rfc3339("2024-02-29T10:01:20+01:00")
        .expect("valid timestamp")
        .with_timezone(&chrono::Utc);
    Persisted {
        id: Id::from(3),
        created_at,
        value: feeds::Feed {
            title: "Svenska Dagbladet".to_string(),
        },
    }
});

static RSS_URL: &str = "https://www.svd.se/feed/articles.rss";

pub async fn crawl(
    http_client: &reqwest::Client,
) -> Result<
    Vec<(
        feeds::Entry,
        Vec<(feeds::FieldName, feeds::LanguageCode, String)>,
    )>,
    Box<dyn std::error::Error + 'static + Send + Sync>,
> {
    let response = http_client.get(RSS_URL).send().await?;
    let bytes = response.bytes().await?;
    let parser = feed_rs::parser::Builder::new()
        .base_uri(Some(RSS_URL))
        .build();
    let entries = parser
        .parse(bytes.to_vec().as_slice())
        .map(|feed| feed.entries)?;
    let entries = entries
        .iter()
        .map(parse_entry)
        .collect::<Result<Vec<_>, _>>()?;
    Ok(entries)
}

#[derive(Debug, thiserror::Error)]
enum ParseError {
    #[error("no description")]
    NoDescription,
    #[error("no title")]
    NoTitle,
    #[error("no link")]
    NoLink,
    #[error("no date")]
    NoDate,
}

fn parse_entry(
    entry: &feed_rs::model::Entry,
) -> Result<
    (
        feeds::Entry,
        Vec<(feeds::FieldName, feeds::LanguageCode, String)>,
    ),
    ParseError,
> {
    let fields = vec![
        entry
            .title
            .as_ref()
            .map(|t| {
                (
                    feeds::FieldName::Title,
                    feeds::LanguageCode::SV,
                    t.content.clone(),
                )
            })
            .ok_or(ParseError::NoTitle)?,
        entry
            .summary
            .as_ref()
            .map(|summary| {
                (
                    feeds::FieldName::Description,
                    feeds::LanguageCode::SV,
                    html2text(&summary.content),
                )
            })
            .ok_or(ParseError::NoDescription)?,
    ];
    let entry = feeds::Entry {
        feed_id: FEED.id,
        href: entry
            .links
            .first()
            .map(|link| link.href.as_str())
            .and_then(|href| href.parse().ok())
            .ok_or(ParseError::NoLink)?,
        published_at: entry
            .updated
            .or(entry.published)
            .ok_or(ParseError::NoDate)?,
    };
    Ok((entry, fields))
}

fn html2text(html: &str) -> String {
    use select::document::Document;
    use select::predicate::Name;

    Document::from(html)
        .find(Name("p"))
        .map(|node| node.first_child())
        .filter_map(|node| node.and_then(|node| node.as_text()))
        .map(ToString::to_string)
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect::<Vec<_>>()
        .join("\n")
}
