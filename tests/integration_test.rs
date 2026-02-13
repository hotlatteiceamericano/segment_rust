#[cfg(test)]
mod test {
    use std::path::PathBuf;

    use rstest::{fixture, rstest};
    use segment_rust::{message::Message, segment::Segment};
    use tempfile::tempdir;

    #[fixture]
    fn temp_dir() -> PathBuf {
        tempdir().unwrap().path().to_owned()
    }

    #[rstest]
    fn test_save_read_message(temp_dir: PathBuf) {
        let mut segment = Segment::new(&temp_dir, 0).expect("cannot create segment from temp dir");
        let message = Message::new("hello integration test!");
        segment
            .write(&message)
            .expect("cannot write message to the integration test segment");

        let message_read = segment
            .read(0)
            .expect("cannot read message at 0 offset from integration test segment");

        assert_eq!(message.content, message_read.content);
    }
}
