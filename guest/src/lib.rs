use serde::{Deserialize, Serialize};
use std::thread;
use std::time::Duration;

wit_bindgen_rust::export!("../spin_kafka.wit");

struct SpinKafka;

#[derive(Serialize, Deserialize)]
struct Message {
    time_to_wait: u64,
    wait: bool,
}

impl spin_kafka::SpinKafka for SpinKafka {
    fn handle_message(
        message: spin_kafka::Message,
    ) -> Result<spin_kafka::MessageAction, spin_kafka::Error> {
        println!("{:#?}", message);
        let message: Message = serde_json::from_str(message.value.as_str()).unwrap();
        if message.wait {
            println!("waiting {} seconds", message.time_to_wait);
            thread::sleep(Duration::from_secs(message.time_to_wait));
            println!("done waiting")
        } else {
            println!("not waiting")
        }
        Ok(spin_kafka::MessageAction::Commit)
    }
}
