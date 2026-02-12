import gzip
import json
import os
import time
import requests
from datetime import datetime

# 配置
REPO_OWNER = "helloamger"
REPO_NAME = "JiNan-Company-Rating"
CATEGORY_ID = "DIC_kwDORKDfUs4C19oY"
OUTPUT_FILE = "discussions.json"
CHECKPOINT_FILE = "discussions_checkpoint.json"
GITHUB_API_URL = "https://api.github.com/graphql"
OUTPUT_GZIP_FILE = "discussions.json.gz"

# 获取 GitHub Token
def get_github_token():
    token = os.environ.get('GITHUB_TOKEN')
    return token

# GraphQL 查询
def get_discussions_query(cursor=None):
    # 如果有 cursor，添加 after 参数
    after_clause = f', after: "{cursor}"' if cursor else ""

    query = f'''
    query {{
      repository(owner: "{REPO_OWNER}", name: "{REPO_NAME}") {{
        discussions(
          first: 100
          categoryId: "{CATEGORY_ID}"
          orderBy: {{field: CREATED_AT, direction: ASC}}
          {after_clause}
        ) {{
          pageInfo {{
            hasNextPage
            endCursor
          }}
          edges {{
            node {{
              number
              bodyHTML
              title
              createdAt
              url
            }}
          }}
        }}
      }}
    }}
    '''
    return query


# 发送 GraphQL 请求，带重试机制
def execute_graphql_with_retry(query, token, max_retries=3, retry_delay=5):
    headers = {
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json',
    }

    for attempt in range(max_retries):
        try:
            response = requests.post(
                GITHUB_API_URL,
                json={'query': query},
                headers=headers,
                timeout=30
            )

            # 检查 HTTP 错误
            response.raise_for_status()

            data = response.json()

            # 检查 GraphQL 错误
            if 'errors' in data:
                error_msg = data['errors'][0].get('message', 'Unknown GraphQL error')
                print(f"GraphQL 错误: {error_msg}")

                # 如果是 rate limit 错误，等待更长时间
                if 'rate limit' in error_msg.lower():
                    reset_time = int(response.headers.get('X-RateLimit-Reset', 0))
                    if reset_time:
                        wait_time = max(reset_time - int(time.time()), 0) + 5
                        print(f"达到速率限制，等待 {wait_time} 秒...")
                        time.sleep(wait_time)
                        continue

                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
                    continue
                else:
                    raise Exception(f"GraphQL 错误: {error_msg}")

            return data

        except requests.exceptions.RequestException as e:
            print(f"请求失败 (尝试 {attempt + 1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                wait_time = retry_delay * (attempt + 1)  # 指数退避
                print(f"等待 {wait_time} 秒后重试...")
                time.sleep(wait_time)
            else:
                raise Exception(f"请求失败，已重试 {max_retries} 次: {e}")

    return None


# 加载检查点
def load_checkpoint():
    if os.path.exists(CHECKPOINT_FILE):
        try:
            with open(CHECKPOINT_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            print(f"加载检查点失败: {e}")
    return {
        'discussions': [],
        'last_cursor': None,
        'has_more': True,
        'total_count': 0
    }


# 保存检查点
def save_checkpoint(checkpoint_data):
    try:
        with open(CHECKPOINT_FILE, 'w', encoding='utf-8') as f:
            json.dump(checkpoint_data, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"保存检查点失败: {e}")


# 保存最终结果
def save_final_result(discussions):
    output_data = {
        'repository': f"{REPO_OWNER}/{REPO_NAME}",
        'category_id': CATEGORY_ID,
        'total_count': len(discussions),
        'fetched_at': datetime.now().isoformat(),
        'discussions': discussions
    }

    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(output_data, f, ensure_ascii=False, indent=2)

    print(f"\n✅ 成功保存 {len(discussions)} 条 discussions 到 {OUTPUT_FILE}")

    # 2. 保存 GZIP 压缩版本（生产用）
    json_str = json.dumps(output_data, ensure_ascii=False)
    json_bytes = json_str.encode('utf-8')

    with gzip.open(OUTPUT_GZIP_FILE, 'wb', compresslevel=9) as f:
        f.write(json_bytes)

    compressed_size = os.path.getsize(OUTPUT_GZIP_FILE)
    compression_ratio = (1 - compressed_size / len(json_bytes)) * 100
    print(f"✅ 已保存 GZIP: {OUTPUT_GZIP_FILE} ({compressed_size} bytes, 压缩率 {compression_ratio:.1f}%)")


# 主函数
def fetch_discussions():
    token = get_github_token()

    # 加载之前的进度
    checkpoint = load_checkpoint()
    all_discussions = checkpoint['discussions']
    cursor = checkpoint['last_cursor']
    has_more = checkpoint['has_more']

    print(f"开始获取 discussions...")
    print(f"仓库: {REPO_OWNER}/{REPO_NAME}")
    print(f"分类 ID: {CATEGORY_ID}")

    if all_discussions:
        print(f"从检查点恢复，已有 {len(all_discussions)} 条记录，继续获取...")

    page_count = 0

    try:
        while has_more:
            page_count += 1
            print(f"\n📄 获取第 {page_count} 页...")

            query = get_discussions_query(cursor)
            data = execute_graphql_with_retry(query, token)

            if not data or 'data' not in data:
                print("⚠️ 未获取到数据，保存当前进度并退出...")
                break

            discussions_data = data['data']['repository']['discussions']
            edges = discussions_data['edges']
            page_info = discussions_data['pageInfo']

            # 处理当前页的数据
            new_discussions = []
            for edge in edges:
                node = edge['node']
                discussion = {
                    'number': node['number'],
                    'title': node['title'],
                    'created_at': node['createdAt'],
                    'url': node['url'],
                    'bodyHTML': node['bodyHTML']
                }
                new_discussions.append(discussion)

            all_discussions.extend(new_discussions)

            # 更新状态
            has_more = page_info['hasNextPage']
            cursor = page_info['endCursor'] if has_more else None

            print(f"本页获取 {len(new_discussions)} 条，总计 {len(all_discussions)} 条")

            # 保存检查点
            checkpoint = {
                'discussions': all_discussions,
                'last_cursor': cursor,
                'has_more': has_more,
                'total_count': len(all_discussions)
            }
            save_checkpoint(checkpoint)

            # 如果还有更多，等待一小段时间避免触发速率限制
            if has_more:
                time.sleep(25)

        # 保存最终结果
        save_final_result(all_discussions)

        # 清理检查点文件（可选）
        if os.path.exists(CHECKPOINT_FILE):
            os.remove(CHECKPOINT_FILE)
            print(f"已清理检查点文件 {CHECKPOINT_FILE}")

        return all_discussions

    except KeyboardInterrupt:
        print("\n\n⚠️ 用户中断，保存当前进度...")
        checkpoint = {
            'discussions': all_discussions,
            'last_cursor': cursor,
            'has_more': has_more if 'has_more' in locals() else True,
            'total_count': len(all_discussions)
        }
        save_checkpoint(checkpoint)
        print(f"进度已保存到 {CHECKPOINT_FILE}，下次运行会自动恢复")
        return all_discussions

    except Exception as e:
        print(f"\n❌ 发生错误: {e}")
        print("保存当前进度...")
        checkpoint = {
            'discussions': all_discussions,
            'last_cursor': cursor if 'cursor' in locals() else None,
            'has_more': has_more if 'has_more' in locals() else True,
            'total_count': len(all_discussions)
        }
        save_checkpoint(checkpoint)
        raise


if __name__ == "__main__":
    try:
        discussions = fetch_discussions()
        print(f"\n🎉 完成！共获取 {len(discussions)} 条 discussions")

        # 显示前 5 条作为预览
        if discussions:
            print("\n预览前 5 条:")
            for i, d in enumerate(discussions[:5], 1):
                print(f"  {i}. #{d['number']}: {d['title'][:50]}{'...' if len(d['title']) > 50 else ''}")

    except Exception as e:
        print(f"\n💥 程序异常退出: {e}")
        exit(1)
