import json
import boto3
import subprocess
import os
import shutil
from datetime import datetime
from pathlib import Path

# AWS S3 클라이언트
s3_client = boto3.client('s3')

# 상수
HLS_SEGMENT_DURATION = 10
TEMP_DIR = '/tmp'

"""
    S3에서 업로드된 비디오 파일을 감지하고 HLS 인코딩하는 Lambda 함수
"""
def lambda_handler(event, context):

    print(">>>>> Start FFmpeg HLS Encoder >>>>>")

    try:
        # 1. S3 이벤트에서 파일 정보 추출
        bucket_name = event['Records'][0]['s3']['bucket']['name']
        file_key = event['Records'][0]['s3']['object']['key']  # videos/original/yy/MM/dd/uuid-filename.mp4

        print(f"Bucket: {bucket_name}, File Key: {file_key}")

        # 2. 파일명과 UUID 추출
        file_name = file_key.split('/')[-1]  # uuid-filename.mp4
        uuid = file_name.split('.')[0]  # uuid-filename → uuid 부분까지

        # 3. 출력 경로 결정 (입력 파일의 날짜 구조 유지)
        date_path = '/'.join(file_key.split('/')[:-1])  # videos/original/yy/MM/dd
        output_s3_prefix = date_path.replace('original', 'hls') + '/' + uuid + '/'

        print(f"UUID: {uuid}")
        print(f"Output S3 Prefix: {output_s3_prefix}")

        # 4. 로컬 임시 폴더 설정
        input_file_path = os.path.join(TEMP_DIR, file_name)
        encoding_dir = os.path.join(TEMP_DIR, uuid)

        # 기존 인코딩 디렉토리가 있으면 삭제
        if os.path.exists(encoding_dir):
            shutil.rmtree(encoding_dir)
        os.makedirs(encoding_dir)

        # 5. S3에서 파일 다운로드
        print(f"Downloading file from S3: {file_key}")
        s3_client.download_file(bucket_name, file_key, input_file_path)
        print(f"Downloaded to: {input_file_path}")

        # 6. FFmpeg로 HLS 인코딩
        print("Starting FFmpeg encoding...")
        encode_to_hls(input_file_path, encoding_dir, uuid)
        print("FFmpeg encoding completed!")

        # 7. 인코딩된 파일들을 S3에 업로드
        print("Uploading encoded files to S3...")
        upload_hls_files_to_s3(bucket_name, encoding_dir, output_s3_prefix)
        print("Upload completed!")

        # 8. 임시 파일 정리
        cleanup(input_file_path, encoding_dir)

        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': '성공적으로 HLS 인코딩이 완료되었습니다.',
                'uuid': uuid,
                'output_path': output_s3_prefix
            })
        }

    except Exception as e:
        print(f"Error occurred: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e)
            })
        }


def encode_to_hls(input_file, output_dir, uuid):
    """
    FFmpeg을 사용하여 비디오를 HLS 형식으로 인코딩
    """
    ffmpeg_path = '/opt/bin/ffmpeg'

    segment_pattern = os.path.join(output_dir, 'segment_%08d.ts')
    index_file = os.path.join(output_dir, 'index.m3u8')

    command = [
        ffmpeg_path,
        '-i', input_file,
        '-c', 'copy',  # 코덱 변환 없음 (빠름)
        '-hls_time', str(HLS_SEGMENT_DURATION),
        '-hls_list_size', '0',
        '-f', 'hls',
        '-hls_segment_filename', segment_pattern,
        '-y',  # 기존 파일 덮어쓰기
        index_file
    ]

    print(f"Running command: {' '.join(command)}")

    # FFmpeg 실행
    result = subprocess.run(command, capture_output=True, text=True)

    print(f"FFmpeg stdout: {result.stdout}")
    if result.stderr:
        print(f"FFmpeg stderr: {result.stderr}")

    if result.returncode != 0:
        raise Exception(f"FFmpeg encoding failed with exit code {result.returncode}")


def upload_hls_files_to_s3(bucket_name, local_dir, s3_prefix):
    """
    로컬 인코딩 디렉토리의 모든 파일을 S3에 업로드
    """
    for file_name in os.listdir(local_dir):
        local_file_path = os.path.join(local_dir, file_name)

        if os.path.isfile(local_file_path):
            s3_key = s3_prefix + file_name
            print(f"Uploading {file_name} to s3://{bucket_name}/{s3_key}")
            s3_client.upload_file(local_file_path, bucket_name, s3_key)


def cleanup(input_file_path, encoding_dir):
    """
    임시 파일 및 디렉토리 정리
    """
    print("Cleaning up temporary files...")

    # 입력 파일 삭제
    if os.path.exists(input_file_path):
        os.remove(input_file_path)
        print(f"Deleted: {input_file_path}")

    # 인코딩 디렉토리 삭제
    if os.path.exists(encoding_dir):
        shutil.rmtree(encoding_dir)
        print(f"Deleted: {encoding_dir}")