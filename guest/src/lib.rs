wit_bindgen_rust::export!("../spin_kafka.wit");

struct SpinKafka;

impl spin_kafka::SpinKafka for SpinKafka {
    fn handle_message(
        message: spin_kafka::Message,
    ) -> Result<spin_kafka::MessageAction, spin_kafka::Error> {
        println!("{:#?}", message);
        Ok(spin_kafka::MessageAction::Commit)
    }
}
