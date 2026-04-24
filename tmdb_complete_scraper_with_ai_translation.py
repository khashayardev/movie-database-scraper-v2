# tmdb_complete_scraper_with_ai_translation.py
import requests
import json
import time
import os
import sys
from datetime import datetime
from typing import List, Dict, Optional

class TMDBAITranslator:
    """کلاس مدیریت ترجمه با Cloudflare Workers AI"""
    
    def __init__(self, account_id: str, api_token: str):
        self.account_id = account_id
        self.api_token = api_token
        self.api_url = f"https://api.cloudflare.com/client/v4/accounts/{account_id}/ai/run/@cf/meta/llama-3.1-8b-instruct"
        self.translation_count = 0
        self.cache = {}  # کش برای جلوگیری از ترجمه تکراری
    
    def translate(self, text: str, field_type: str = "general") -> str:
        """
        ترجمه هوشمند متن بر اساس نوع فیلد
        field_type می‌تونه باشه:
        - title: عنوان فیلم
        - overview: خلاصه داستان
        - tagline: شعار تبلیغاتی
        - genre: ژانر
        - keyword: کلمه کلیدی
        - character: نام کاراکتر
        - job: شغل/نقش
        - general: متن عمومی
        """
        if not text or not isinstance(text, str) or not text.strip():
            return text
        
        # چک کردن کش
        cache_key = f"{text}_{field_type}"
        if cache_key in self.cache:
            return self.cache[cache_key]
        
        # تعیین prompt مناسب
        prompts = {
            "title": f"""You are a professional movie translator. Translate this movie title to Persian (Farsi).

Important rules:
1. If the movie has an official Persian title, use that exact title
2. Be creative but stay faithful to the original meaning
3. Persian titles should be catchy and appealing
4. Keep proper nouns in English if they're well-known
5. Return ONLY the translated title, nothing else

English title: "{text}"
Persian title:""",

            "overview": f"""You are a professional movie critic and translator. Translate this movie overview to natural, fluent Persian (Farsi).

Rules:
1. Write like a Persian movie critic - engaging and descriptive
2. Use natural Persian expressions, not literal translations
3. Keep the excitement and tone of the original
4. Movie terminology should be in Persian
5. Keep character names in English
6. Return ONLY the translation

English: "{text}"
Persian:""",

            "tagline": f"""Translate this movie tagline to Persian (Farsi).
Make it punchy, memorable, and natural in Persian.
Return ONLY the translation.

English: "{text}"
Persian:""",

            "genre": f"""Translate this movie genre to Persian (Farsi).
Use standard Persian movie genre terminology.
Return ONLY the translation.

English: "{text}"
Persian:""",

            "keyword": f"""Translate this movie keyword/tag to Persian (Farsi).
Keep it concise and use common Persian terms.
Return ONLY the translation.

English: "{text}"
Persian:""",

            "character": f"""Translate this character role/description to Persian (Farsi).
Keep proper names as is, translate descriptions naturally.
Return ONLY the translation.

English: "{text}"
Persian:""",

            "job": f"""Translate this movie production role to Persian (Farsi).
Use standard Persian film industry terminology.
Return ONLY the translation.

English: "{text}"
Persian:""",

            "general": f"""Translate this text to natural, fluent Persian (Farsi).
Keep proper nouns in English. Use natural Persian expressions.
Return ONLY the translation.

English: "{text}"
Persian:"""
        }
        
        prompt = prompts.get(field_type, prompts["general"])
        
        try:
            headers = {
                "Authorization": f"Bearer {self.api_token}",
                "Content-Type": "application/json"
            }
            
            payload = {
                "messages": [
                    {
                        "role": "system",
                        "content": "You are an expert Persian translator specializing in film and cinema. Your translations are always accurate, natural, and culturally appropriate for Iranian audiences."
                    },
                    {
                        "role": "user",
                        "content": prompt
                    }
                ],
                "max_tokens": 1024,
                "temperature": 0.3  # دمای پایین برای ترجمه دقیق‌تر
            }
            
            response = requests.post(
                self.api_url,
                headers=headers,
                json=payload,
                timeout=30
            )
            
            if response.status_code == 200:
                result = response.json()
                translated = result['result']['response'].strip()
                
                # ذخیره در کش
                self.cache[cache_key] = translated
                self.translation_count += 1
                
                if self.translation_count % 10 == 0:
                    print(f"         🌐 {self.translation_count} ترجمه انجام شد")
                
                return translated
            else:
                print(f"         ⚠️ خطای API ترجمه: {response.status_code}")
                return text
                
        except Exception as e:
            print(f"         ❌ خطا در ترجمه: {str(e)[:50]}")
            return text


class TMDBCompleteScraper:
    def __init__(self, access_token: str, translator: TMDBAITranslator):
        self.base_url = "https://api.themoviedb.org/3"
        self.headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json;charset=utf-8"
        }
        self.translator = translator
        self.append_to_response = "videos,images,credits,keywords,recommendations,similar,external_ids,release_dates,translations"
        
        self.stats = {
            'total_movies': 0,
            'years_processed': 0,
            'api_calls': 0,
            'translations': 0
        }

    def translate_movie_fields(self, movie: Dict) -> Dict:
        """ترجمه تمام فیلدهای مهم فیلم"""
        
        print(f"         📝 ترجمه فیلدهای فارسی...")
        
        # ترجمه عنوان
        if movie.get('title'):
            movie['title_fa'] = self.translator.translate(movie['title'], 'title')
        
        # ترجمه overview
        if movie.get('overview'):
            movie['overview_fa'] = self.translator.translate(movie['overview'], 'overview')
        
        # ترجمه tagline
        if movie.get('tagline'):
            movie['tagline_fa'] = self.translator.translate(movie['tagline'], 'tagline')
        
        # ترجمه ژانرها
        if movie.get('genres'):
            for genre in movie['genres']:
                if genre.get('name'):
                    genre['name_fa'] = self.translator.translate(genre['name'], 'genre')
        
        # ترجمه کلمات کلیدی
        if movie.get('keywords_summary'):
            movie['keywords_summary_fa'] = [
                self.translator.translate(kw, 'keyword') 
                for kw in movie['keywords_summary']
            ]
        
        # ترجمه اطلاعات بازیگران
        if movie.get('credits_summary'):
            summary = movie['credits_summary']
            
            # ترجمه نقش کارگردان و نویسنده
            summary['director_fa'] = f"کارگردان: {summary['director']}" if summary.get('director') else None
            summary['writer_fa'] = f"نویسنده: {summary['writer']}" if summary.get('writer') else None
            
            # ترجمه بازیگران
            if summary.get('cast'):
                for actor in summary['cast']:
                    if actor.get('character'):
                        actor['character_fa'] = self.translator.translate(
                            actor['character'], 'character'
                        )
        
        # ترجمه نام شرکت‌های تولید
        if movie.get('production_companies'):
            for company in movie['production_companies']:
                if company.get('name'):
                    company['name_fa'] = self.translator.translate(company['name'], 'general')
        
        # ترجمه کشورهای تولید
        if movie.get('production_countries'):
            for country in movie['production_countries']:
                if country.get('name'):
                    country['name_fa'] = self.translator.translate(country['name'], 'general')
        
        # ترجمه زبان‌ها
        if movie.get('spoken_languages'):
            for lang in movie['spoken_languages']:
                if lang.get('english_name'):
                    lang['name_fa'] = self.translator.translate(lang['english_name'], 'general')
        
        # ترجمه recommendations
        if movie.get('recommendations') and movie['recommendations'].get('results'):
            for rec in movie['recommendations']['results']:
                if rec.get('title'):
                    rec['title_fa'] = self.translator.translate(rec['title'], 'title')
                if rec.get('overview'):
                    rec['overview_fa'] = self.translator.translate(rec['overview'], 'overview')
        
        # ترجمه similar movies
        if movie.get('similar') and movie['similar'].get('results'):
            for sim in movie['similar']['results']:
                if sim.get('title'):
                    sim['title_fa'] = self.translator.translate(sim['title'], 'title')
                if sim.get('overview'):
                    sim['overview_fa'] = self.translator.translate(sim['overview'], 'overview')
        
        # ترجمه اطلاعات انتشار در کشورها
        if movie.get('release_dates') and movie['release_dates'].get('results'):
            for release in movie['release_dates']['results']:
                if release.get('release_dates'):
                    for rd in release['release_dates']:
                        if rd.get('descriptors'):
                            rd['descriptors_fa'] = [
                                self.translator.translate(d, 'general') 
                                for d in rd['descriptors']
                            ]
        
        self.stats['translations'] = self.translator.translation_count
        
        return movie

    def fetch_movies_by_year(self, year: int, min_votes: int = 100, min_rating: float = 7.0, limit: int = 500) -> List[Dict]:
        """دریافت لیست فیلم‌های یک سال با فیلتر امتیاز"""
        params = {
            "primary_release_year": year,
            "sort_by": "vote_average.desc",
            "vote_count.gte": min_votes,
            "vote_average.gte": min_rating,
            "include_adult": False
        }
        
        print(f"   🔍 سال {year}: حداقل {min_votes} رای | امتیاز >= {min_rating}")
        
        all_movies = []
        page = 1
        max_pages = 25
        
        while page <= max_pages and len(all_movies) < limit:
            params['page'] = page
            try:
                resp = requests.get(f"{self.base_url}/discover/movie", headers=self.headers, params=params, timeout=15)
                self.stats['api_calls'] += 1
                
                if resp.status_code == 200:
                    data = resp.json()
                    movies = data.get('results', [])
                    if not movies: break
                    all_movies.extend(movies)
                    total_pages = min(data.get('total_pages', 1), max_pages)
                    print(f"      📄 صفحه {page}/{total_pages} - {len(movies)} فیلم (مجموع: {len(all_movies)})")
                    page += 1
                    time.sleep(0.1)
                elif resp.status_code == 429:
                    print(f"      ⚠️ Rate limit! 2 ثانیه صبر...")
                    time.sleep(2)
                else:
                    print(f"      ⚠️ خطا {resp.status_code}")
                    break
            except Exception as e:
                print(f"      ❌ خطا: {e}")
                break
        
        unique_movies = {m['id']: m for m in all_movies}.values()
        result = sorted(list(unique_movies), key=lambda x: x.get('vote_average', 0), reverse=True)[:limit]
        print(f"      ✅ {len(result)} فیلم با امتیاز >= {min_rating} برای سال {year}")
        return result

    def get_complete_movie_details(self, movie_id: int) -> Dict:
        """
        دریافت کامل‌ترین اطلاعات ممکن برای یک فیلم
        """
        params = {
            "append_to_response": self.append_to_response,
            "language": "fa-IR"
        }
        
        try:
            resp = requests.get(
                f"{self.base_url}/movie/{movie_id}",
                headers=self.headers,
                params=params,
                timeout=30
            )
            self.stats['api_calls'] += 1
            
            if resp.status_code == 200:
                data = resp.json()
                
                # اضافه کردن لینک‌های کامل تصاویر
                if data.get('poster_path'):
                    data['poster_url'] = f"https://image.tmdb.org/t/p/w500{data['poster_path']}"
                if data.get('backdrop_path'):
                    data['backdrop_url'] = f"https://image.tmdb.org/t/p/w1280{data['backdrop_path']}"
                
                # خلاصه‌سازی credits
                if data.get('credits'):
                    director = next((c['name'] for c in data['credits'].get('crew', []) if c['job'] == 'Director'), None)
                    writer = next((c['name'] for c in data['credits'].get('crew', []) if c['job'] == 'Screenplay'), None)
                    main_cast = [{'name': c['name'], 'character': c['character']} for c in data['credits'].get('cast', [])[:10]]
                    
                    data['credits_summary'] = {
                        'director': director,
                        'writer': writer,
                        'cast': main_cast,
                        'total_cast': len(data['credits'].get('cast', [])),
                        'total_crew': len(data['credits'].get('crew', []))
                    }
                
                # خلاصه keywords
                if data.get('keywords') and data['keywords'].get('keywords'):
                    data['keywords_summary'] = [k['name'] for k in data['keywords']['keywords'][:15]]
                
                # اطلاعات certification
                if data.get('release_dates') and data['release_dates'].get('results'):
                    us_release = next((r for r in data['release_dates']['results'] if r['iso_3166_1'] == 'US'), None)
                    if us_release and us_release.get('release_dates'):
                        data['us_certification'] = us_release['release_dates'][0].get('certification')
                
                # حالا ترجمه تمام فیلدها
                data = self.translate_movie_fields(data)
                
                return data
            return {}
        except Exception as e:
            print(f"      ❌ خطا در دریافت جزئیات فیلم {movie_id}: {e}")
            return {}

    def scrape_yearly_archive(self, start_year: int, end_year: int, min_rating: float = 7.0):
        """اجرای اصلی اسکرپینگ"""
        print(f"\n🎬 شروع ساخت آرشیو کامل با ترجمه هوش مصنوعی")
        print(f"📅 از {start_year} تا {end_year}")
        print(f"⭐ فیلتر: فیلم‌های با امتیاز >= {min_rating}")
        print(f"🌐 ترجمه با Cloudflare Workers AI (Llama 3.1)")
        print("="*60)
        
        archive = {
            'metadata': {
                'start_year': start_year,
                'end_year': end_year,
                'total_years': end_year - start_year + 1,
                'min_rating': min_rating,
                'min_votes': 100,
                'extraction_date': datetime.now().isoformat(),
                'source': 'TMDB Complete Scraper',
                'translator': 'Cloudflare Workers AI (Llama 3.1 8B)',
                'translation_quality': 'AI-Powered Natural Persian Translation',
                'description': 'آرشیو کامل فیلم‌ها با ترجمه فارسی هوش مصنوعی - تمام فیلدهای متنی به فارسی روان ترجمه شده‌اند'
            },
            'movies': []
        }
        
        for year in range(start_year, end_year + 1):
            print(f"\n📅 سال {year}:")
            
            # مرحله 1: دریافت لیست فیلم‌های سال
            movies = self.fetch_movies_by_year(year, min_votes=100, min_rating=min_rating, limit=500)
            
            # مرحله 2: دریافت جزئیات و ترجمه
            detailed_movies = []
            total = len(movies)
            for i, movie in enumerate(movies, 1):
                print(f"      🔄 [{i}/{total}] دریافت و ترجمه: {movie.get('title', 'Unknown')[:45]}...")
                details = self.get_complete_movie_details(movie['id'])
                if details:
                    detailed_movies.append(details)
                    print(f"         ✅ تکمیل شد - {self.translator.translation_count} ترجمه")
                time.sleep(0.2)
            
            archive['movies'].extend(detailed_movies)
            self.stats['total_movies'] += len(detailed_movies)
            self.stats['years_processed'] += 1
            print(f"   ✅ {len(detailed_movies)} فیلم کامل و ترجمه شده برای سال {year}")
        
        archive['metadata']['statistics'] = self.stats
        self.save_archive(archive)
        return archive

    def save_archive(self, archive: Dict):
        """ذخیره آرشیو نهایی"""
        start = archive['metadata']['start_year']
        end = archive['metadata']['end_year']
        filename = f"movies_archive_translated_{start}_{end}.json"
        
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(archive, f, ensure_ascii=False, indent=2)
        
        print(f"\n💾 آرشیو نهایی در فایل {filename} ذخیره شد.")
        print(f"\n📊 آمار نهایی:")
        print(f"   📅 سال‌های پردازش شده: {archive['metadata']['total_years']}")
        print(f"   🎬 تعداد کل فیلم‌ها: {archive['metadata']['statistics']['total_movies']}")
        print(f"   🌐 تعداد درخواست‌های TMDB: {archive['metadata']['statistics']['api_calls']}")
        print(f"   🌍 تعداد ترجمه‌های انجام شده: {self.translator.translation_count}")
        print(f"   ⭐ حداقل امتیاز: {archive['metadata']['min_rating']}")
        print(f"\n✨ تمام فیلدهای زیر به فارسی ترجمه شده‌اند:")
        print(f"   • عنوان فیلم (title_fa)")
        print(f"   • خلاصه داستان (overview_fa)")
        print(f"   • شعار تبلیغاتی (tagline_fa)")
        print(f"   • ژانرها (genres[].name_fa)")
        print(f"   • کلمات کلیدی (keywords_summary_fa)")
        print(f"   • نام کاراکترها (character_fa)")
        print(f"   • نام شرکت‌ها و کشورها")
        print(f"   • فیلم‌های پیشنهادی و مشابه")
        return filename


def main():
    print("🎬 TMDB Complete Scraper with AI Translation")
    print("🌐 Powered by Cloudflare Workers AI")
    print("="*50)
    
    # دریافت توکن‌ها
    TMDB_TOKEN = os.environ.get('TMDB_ACCESS_TOKEN')
    CF_ACCOUNT_ID = os.environ.get('CF_ACCOUNT_ID')
    CF_API_TOKEN = os.environ.get('CF_API_TOKEN')
    
    if not TMDB_TOKEN:
        print("❌ خطا: TMDB_ACCESS_TOKEN تنظیم نشده است")
        print("لطفاً این متغیر محیطی را تنظیم کنید:")
        print("export TMDB_ACCESS_TOKEN='your_token_here'")
        sys.exit(1)
    
    if not CF_ACCOUNT_ID or not CF_API_TOKEN:
        print("❌ خطا: تنظیمات Cloudflare کامل نیست")
        print("لطفاً این متغیرهای محیطی را تنظیم کنید:")
        print("export CF_ACCOUNT_ID='your_account_id'")
        print("export CF_API_TOKEN='your_api_token'")
        sys.exit(1)
    
    # دریافت پارامترها
    if len(sys.argv) >= 3:
        start_year = int(sys.argv[1])
        end_year = int(sys.argv[2])
    else:
        start_year = 2026
        end_year = 2026
    
    min_rating = 7.0
    if len(sys.argv) >= 4:
        min_rating = float(sys.argv[3])
    
    # ایجاد translator و scraper
    translator = TMDBAITranslator(CF_ACCOUNT_ID, CF_API_TOKEN)
    scraper = TMDBCompleteScraper(TMDB_TOKEN, translator)
    
    print(f"\n📋 تنظیمات:")
    print(f"   سال شروع: {start_year}")
    print(f"   سال پایان: {end_year}")
    print(f"   حداقل امتیاز: {min_rating}")
    print(f"   مدل AI: Llama 3.1 8B")
    print("\n⏳ شروع فرآیند...")
    
    # اجرای اسکرپینگ
    scraper.scrape_yearly_archive(start_year, end_year, min_rating)
    
    print("\n✅ فرآیند با موفقیت کامل شد!")
    print("📁 فایل JSON با ترجمه فارسی آماده استفاده است.")


if __name__ == "__main__":
    main()
