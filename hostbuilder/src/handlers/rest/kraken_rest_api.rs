use anyhow::{anyhow, Result};
use base64::{prelude::BASE64_STANDARD, Engine};
use hmac::{Hmac, Mac};
use reqwest::{
    header::{HeaderMap, HeaderValue, CONTENT_TYPE},
    Client,
};
use sha2::{Digest, Sha256, Sha512};
use std::{
    env,
    time::{SystemTime, UNIX_EPOCH},
};
use url::Url;

use crate::handlers::structs::responses::{parse_message, Response, TokenResponse};

pub struct KrakenRestHandler;

impl KrakenRestHandler {
    pub async fn authenticate(endpoint: &str) -> Result<TokenResponse> {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_millis() as u64;

        let post_data = format!("nonce={}", nonce);

        let url = Url::parse(endpoint).expect("Failed to parse URL");

        let api_secret = env::var("SECRET_KEY").expect("SECRET_KEY must be set");

        let signature = generate_kraken_signature(url.path(), nonce, &post_data, &api_secret);
        let client = Client::new();
        let api_key = env::var("API_KEY").expect("API_KEY must be set");

        let body = post_data;
        // Create the headers
        let mut headers = HeaderMap::new();
        headers.insert("API-Key", HeaderValue::from_str(&api_key)?);
        headers.insert("API-Sign", HeaderValue::from_str(&signature)?);
        headers.insert(
            CONTENT_TYPE,
            HeaderValue::from_static("application/x-www-form-urlencoded"),
        );
        
        match client.post(url).headers(headers).body(body).send().await {
            Ok(res) => {
                let body = res.text().await.expect("Failed to parse API response");

                match parse_message(&body) {
                    Ok(Response::TokenResponse(api_response)) => {
                        println!("Successfully received TokenResponse: {:?}", api_response);
                        Ok(api_response)
                    }
                    Ok(other_response) => {
                        // Return an error because we expected a TokenResponse
                        Err(anyhow!(
                            "Expected TokenResponse but got {:?}",
                            other_response
                        ))
                    }
                    Err(e) => {
                        // Handle parsing error
                        println!("Error: Failed to parse message: {}", e);
                        Err(e.into())
                    }
                }
            }
            Err(e) => {
                println!("Error: {:?}", e);
                Err(e.into())
            }
        }
    }
}

type HmacSha512 = Hmac<Sha512>;
fn generate_kraken_signature(
    api_path: &str,
    nonce: u64,
    post_data: &str,
    api_secret: &str,
) -> String {
    // 1. Calculate SHA256 of the nonce + POST data
    let mut sha256 = Sha256::new();
    sha256.update(nonce.to_string() + post_data);
    let sha256_hash = sha256.finalize();
    // 2. Decode the API secret from base64
    let decoded_secret = BASE64_STANDARD
        .decode(api_secret)
        .expect("Failed to decode API secret from base64");
    // 3. Create an HMAC-SHA512 instance with the decoded key
    let mut mac =
        HmacSha512::new_from_slice(&decoded_secret).expect("HMAC can take key of any size");
    // Update the HMAC with the API path and SHA256 hash
    mac.update(api_path.as_bytes());
    mac.update(&sha256_hash);
    // 4. Finalize the HMAC calculation and encode the result into base64
    let hmac_result = mac.finalize();
    let hmac_bytes = hmac_result.into_bytes();
    BASE64_STANDARD.encode(hmac_bytes)
}
