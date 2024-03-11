use select::document::Document;
use select::node::Node;
use select::predicate::{Class, Name, Predicate};

use crate::feeds;
use crate::id::Id;
use crate::persisted::Persisted;

pub static FEED: once_cell::sync::Lazy<Persisted<feeds::Feed>> = once_cell::sync::Lazy::new(|| {
    let created_at = chrono::DateTime::parse_from_rfc3339("2024-02-29T10:01:20+01:00")
        .expect("valid timestamp")
        .with_timezone(&chrono::Utc);
    Persisted {
        id: Id::from(2),
        created_at,
        value: feeds::Feed {
            title: "Dagens Nyheter".to_string(),
        },
    }
});

pub async fn crawl(
    http_client: &reqwest::Client,
) -> Result<
    Vec<(
        feeds::Entry,
        Vec<(feeds::FieldName, feeds::LanguageCode, String)>,
    )>,
    Box<dyn std::error::Error + 'static + Send + Sync>,
> {
    let response = http_client.get("https://www.dn.se/direkt/").send().await?;
    let bytes = response.bytes().await?;
    let body = std::str::from_utf8(&bytes)?;

    let doc = Document::from(body);
    let entries = doc
        .find(Name("article").and(Class("direkt-post")))
        .filter_map(|e| {
            parse_entry(e)
                .map_err(|error| {
                    tracing::warn!(?error, "failed to parse dn entry");
                    error
                })
                .ok()
        })
        .collect::<Vec<_>>();

    Ok(entries)
}

#[derive(Debug, thiserror::Error)]
enum ParseError {
    #[error("no link ")]
    NoLink,
    #[error("no publish date")]
    NoPublishDate,
    #[error("no title")]
    NoTitle,
    #[error("no content")]
    NoContent,
}

fn parse_entry(
    node: Node<'_>,
) -> Result<
    (
        feeds::Entry,
        Vec<(feeds::FieldName, feeds::LanguageCode, String)>,
    ),
    ParseError,
> {
    let fields = vec![
        (
            feeds::FieldName::Title,
            feeds::LanguageCode::SV,
            node.find(Name("h2"))
                .next()
                .and_then(|node| node.first_child())
                .and_then(|node| node.as_text())
                .map(std::string::ToString::to_string)
                .ok_or(ParseError::NoTitle)?,
        ),
        (
            feeds::FieldName::Description,
            feeds::LanguageCode::SV,
            node.find(Class("direkt-post__content"))
                .next()
                .map(|node| node.children())
                .map(|children| {
                    children
                        .filter_map(|node| {
                            node.first_child()
                                .and_then(|node| node.as_text())
                                .map(std::string::ToString::to_string)
                        })
                        .collect::<Vec<_>>()
                        .join("\n")
                })
                .ok_or(ParseError::NoContent)?,
        ),
    ];
    let entry = feeds::Entry {
        feed_id: FEED.id,
        href: node
            .find(Class("direkt-post__share-button"))
            .next()
            .and_then(|node| node.attr("data-link"))
            .and_then(|href| format!("https://www.dn.se{href}").parse().ok())
            .ok_or(ParseError::NoLink)?,
        published_at: node
            .find(Class("direkt-post__update-time"))
            .next()
            .or(node.find(Class("direkt-post__publication-time")).next())
            .and_then(|node| node.attr("datetime"))
            .and_then(|date| chrono::DateTime::parse_from_rfc3339(date).ok())
            .map(|date| date.with_timezone(&chrono::Utc))
            .ok_or(ParseError::NoPublishDate)?,
    };
    Ok((entry, fields))
}
