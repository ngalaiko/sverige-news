use std::collections::HashSet;

use reqwest_middleware::{ClientBuilder, ClientWithMiddleware};
use reqwest_retry::{
    policies::ExponentialBackoff, RetryTransientMiddleware, Retryable, RetryableStrategy,
};
use reqwest_tracing::TracingMiddleware;

#[derive(Clone)]
pub struct Client {
    base_url: url::Url,
    http_client: ClientWithMiddleware,
}

struct RetryStatusCodes(HashSet<reqwest::StatusCode>);

impl RetryStatusCodes {
    fn new(status_codes: Vec<reqwest::StatusCode>) -> Self {
        Self(status_codes.into_iter().collect())
    }
}

impl RetryableStrategy for RetryStatusCodes {
    fn handle(&self, res: &reqwest_middleware::Result<reqwest::Response>) -> Option<Retryable> {
        match res {
            Ok(response) if self.0.contains(&response.status()) => Some(Retryable::Transient),
            Ok(_) => None,
            Err(error) => reqwest_retry::default_on_request_failure(error),
        }
    }
}

impl Client {
    pub fn new(base_url: &url::Url, token: &str) -> Self {
        let retry_policy = ExponentialBackoff::builder().build_with_max_retries(3);
        let http_client = {
            let mut headers = reqwest::header::HeaderMap::new();
            headers.insert(
                reqwest::header::AUTHORIZATION,
                reqwest::header::HeaderValue::from_str(&format!("Bearer {token}"))
                    .expect("invalid authorization header value"),
            );
            let client = reqwest::ClientBuilder::new()
                .default_headers(headers)
                .build()
                .expect("failed to build reqwest client");
            ClientBuilder::new(client)
                .with(RetryTransientMiddleware::new_with_policy_and_strategy(
                    retry_policy,
                    RetryStatusCodes::new(vec![reqwest::StatusCode::SERVICE_UNAVAILABLE]),
                ))
                .with(TracingMiddleware::default())
                .build()
        };
        Self {
            base_url: base_url.clone(),
            http_client,
        }
    }

    pub async fn comptetions(
        &self,
        task: &str,
        input: &str,
    ) -> Result<String, Box<dyn std::error::Error + 'static + Send + Sync>> {
        #[derive(Debug, serde::Deserialize)]
        struct ChatCompletionMessage {
            content: String,
        }

        #[derive(Debug, serde::Deserialize)]
        struct ChatCompletionChoice {
            message: ChatCompletionMessage,
        }

        #[derive(Debug, serde::Deserialize)]
        struct ChatCompletionResponse {
            choices: Vec<ChatCompletionChoice>,
        }

        let endpoint = self
            .base_url
            .join("/v1/chat/completions")
            .expect("invald chat completions endpoint");
        let body = serde_json::json!({
            "model": "gpt-3.5-turbo",
            "messages": [
                {"role": "system", "content": task},
                {"role": "user", "content": input}
            ],
            "temperature": 0,
        });

        let response = self
            .http_client
            .post(endpoint)
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .body(serde_json::to_string(&body)?)
            .send()
            .await?;

        let response_bytes = response.bytes().await?;

        let response = serde_json::from_slice::<Response<ChatCompletionResponse>>(&response_bytes);
        match response {
            Ok(Response::Ok(completion)) => Ok(completion.choices[0].message.content.clone()),
            Ok(Response::Error { error }) => Err(error.into()),
            Err(error) => Err(error.into()),
        }
    }

    #[tracing::instrument(skip(self))]
    pub async fn embeddings(
        &self,
        input: &str,
    ) -> Result<Vec<f32>, Box<dyn std::error::Error + 'static + Send + Sync>> {
        #[derive(Debug, serde::Deserialize)]
        struct ListResponse<T> {
            data: Vec<T>,
        }

        #[derive(Debug, serde::Deserialize)]
        struct EmbeddingResponse {
            embedding: Vec<f32>,
        }

        let endpoint = self
            .base_url
            .join("/v1/embeddings")
            .expect("invald embeddngs endpoint");
        let body = serde_json::json!({"model": "text-embedding-3-large", "input": input});

        let response = self
            .http_client
            .post(endpoint)
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .body(serde_json::to_string(&body)?)
            .send()
            .await?;

        let response_bytes = response.bytes().await?;

        let response =
            serde_json::from_slice::<Response<ListResponse<EmbeddingResponse>>>(&response_bytes);

        match response {
            Ok(Response::Ok(list)) => Ok(list.data[0].embedding.clone()),
            Ok(Response::Error { error }) => Err(error.into()),
            Err(error) => Err(error.into()),
        }
    }
}

#[derive(Debug, serde::Deserialize, thiserror::Error)]
#[error("{message}")]
pub struct ErrorResponse {
    message: String,
}

#[derive(Debug, serde::Deserialize)]
#[serde(untagged)]
enum Response<T> {
    Ok(T),
    Error { error: ErrorResponse },
}

pub struct Translator<'a> {
    client: &'a Client,
}

impl<'a> Translator<'a> {
    pub fn new(client: &'a Client) -> Self {
        Self { client }
    }

    #[tracing::instrument(skip(self))]
    pub async fn translate_sv_to_en(
        &self,
        value: &str,
    ) -> Result<String, Box<dyn std::error::Error + 'static + Send + Sync>> {
        let task = "You are a highly skilled and concise professional translator. When you receive a sentence in Swedish, your task is to translate it into English. VERY IMPORTANT: Do not output any notes, explanations, alternatives or comments after or before the translation.";
        self.client.comptetions(task, value).await
    }
}
