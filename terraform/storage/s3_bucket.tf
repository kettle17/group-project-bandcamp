resource "aws_s3_bucket" "daily-pdf-report-s3-bucket" {
    bucket = "c17-tracktion-daily-reports-and-images"
    force_destroy = true
}
