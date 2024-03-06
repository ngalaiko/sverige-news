#[derive(Clone)]
pub struct Client {
    base_url: url::Url,
    http_client: reqwest::Client,
}

impl Client {
    pub fn new(base_url: &url::Url, token: &str) -> Self {
        let http_client = {
            let mut headers = reqwest::header::HeaderMap::new();
            headers.insert(
                reqwest::header::AUTHORIZATION,
                reqwest::header::HeaderValue::from_str(&format!("Bearer {token}"))
                    .expect("invalid authorization header value"),
            );
            reqwest::ClientBuilder::new()
                .default_headers(headers)
                .build()
                .expect("failed to build reqwest client")
        };
        Self {
            base_url: base_url.clone(),
            http_client,
        }
    }

    pub async fn comptetions(&self, task: &str, input: &str) -> Result<String, Error> {
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
            .body(serde_json::to_string(&body).map_err(Error::Ser)?)
            .send()
            .await
            .map_err(Error::Reqwest)?;

        let response_bytes = response.bytes().await.map_err(Error::Reqwest)?;

        let response = serde_json::from_slice::<Response<ChatCompletionResponse>>(&response_bytes)
            .map_err(Error::De);

        match response {
            Ok(Response::Ok(completion)) => Ok(completion.choices[0].message.content.clone()),
            Ok(Response::Error { error }) => Err(Error::Api(error)),
            Err(e) => Err(e),
        }
    }

    #[tracing::instrument(skip(self))]
    pub async fn embeddings(&self, input: &str) -> Result<Vec<f32>, Error> {
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
            .body(serde_json::to_string(&body).map_err(Error::Ser)?)
            .send()
            .await
            .map_err(Error::Reqwest)?;

        let response_bytes = response.bytes().await.map_err(Error::Reqwest)?;

        let response =
            serde_json::from_slice::<Response<ListResponse<EmbeddingResponse>>>(&response_bytes)
                .map_err(Error::De);

        match response {
            Ok(Response::Ok(list)) => Ok(list.data[0].embedding.clone()),
            Ok(Response::Error { error }) => Err(Error::Api(error)),
            Err(e) => Err(e),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Reqwest(reqwest::Error),
    #[error(transparent)]
    Ser(serde_json::Error),
    #[error(transparent)]
    De(serde_json::Error),
    #[error(transparent)]
    Api(ErrorResponse),
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
    pub async fn translate_sv_to_en(&self, value: &str) -> Result<String, Error> {
        let task = "You are a highly skilled and concise professional translator. When you receive a sentence in Swedish, your task is to translate it into English. VERY IMPORTANT: Do not output any notes, explanations, alternatives or comments after or before the translation.";
        self.client.comptetions(task, value).await
    }
}
