from dataclasses import dataclass

@dataclass
class S3Dataset:
    bucket: str
    key_template: str  # e.g., "curated/events/date={ds}/data.jsonl"

    @property
    def namespace(self) -> str:
        return f"s3://{self.bucket}"

    @property
    def name(self) -> str:
        return self.key_template

    def render_key(self, **params) -> str:
        return self.key_template.format(**params)

    def uri(self, **params) -> str:
        return f"s3://{self.bucket}/{self.render_key(**params)}"