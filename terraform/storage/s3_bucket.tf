resource "aws_s3_bucket" "long-term-storage-s3-bucket" {
    bucket = "c17-tracktion-daily-reports"
    force_destroy = true
}
