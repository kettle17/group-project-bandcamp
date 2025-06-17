# if __name__ == "__main__":
#     conn = get_connection(
#         ENV['DB_HOST'],
#         ENV['DB_NAME'],
#         ENV['DB_USERNAME'],
#         ENV['DB_PASSWORD'],
#         ENV['DB_PORT']
#     )

#     sale_df = load_sale_data(conn)
#     album_df = load_album_data(conn)

#     st.title("🎶 Live Data Insights 🎶")
#     st.subheader("🔍 Filters")

#     geo_df = geocode_countries(sale_df)

#     st.map(geo_df, size=20, color="#0044ff")

#     st.title("🎨 Album Art Gallery")

#     # for _, row in album_df.iterrows():
#     #     st.image(row['art_url'], width=300,
#     #              caption=f"{row['artist_name']} – {row['album_name']} (${row['sold_for']})")
#     #     st.markdown(
#     #         f"[Open on Bandcamp]({row['url']})", unsafe_allow_html=True)
#     #     st.markdown("---")

#     image_urls = [album_df["art_url"]
#                   ]

#     st.image(image_urls, caption=["Image 1", "Image 2", "Image 3"], width=200)

#     st.title("☁️ Genre Word Cloud")
#     st.image("/mnt/data/tag_wordcloud.png")
