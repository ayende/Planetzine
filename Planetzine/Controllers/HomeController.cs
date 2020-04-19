﻿using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Threading.Tasks;
using System.Web;
using System.Web.Mvc;
using Planetzine.Models;
using Planetzine.Common;

namespace Planetzine.Controllers
{
    public class HomeController : Controller
    {
        [HttpGet]
        public async Task<ActionResult> Index(string tag, string author, string freeTextSearch)
        {
            var articles = new Index();
            if (!string.IsNullOrEmpty(tag))
                articles.Items = await Article.SearchByTag(tag);
            else if (!string.IsNullOrEmpty(author))
                articles.Items = await Article.SearchByAuthor(author);
            else if (!string.IsNullOrEmpty(freeTextSearch))
                articles.Items = await Article.SearchByFreetext(freeTextSearch);
            else
                articles.Items = await Article.GetAll();

            //ViewBag.ConsumedRUs = DbHelper.RequestCharge - initialRequestCharge;
            return View(articles);
        }

        [HttpGet]
        public ActionResult About()
        {
            return View();
        }

        [HttpGet]
        public async Task<ActionResult> View(Guid articleId, string author)
        {
            var article = await Article.Read(articleId, author);
            return View(article);
        }

        [HttpGet]
        public async Task<ActionResult> Diagnostics()
        {
            await Planetzine.MvcApplication.DatabaseReady.Task; // Make sure database and collection is created before continuing

            var diagnostics = new Diagnostics();
            diagnostics.Results = DbHelper.Diagnostics();
            return View(diagnostics);
        }

        [HttpPost]
        public async Task<ActionResult> Diagnostics(string button)
        {
            button = button.ToLower();
            if (button == "delete")
            {
                await DbHelper.DeleteDatabaseAsync();
                ViewBag.Message = "Database deleted! It will be recreated next time you restart the application.";
            }
            if (button == "reset")
            {
                //await DbHelper.DeleteCollection(Article.CollectionId);
                //await DbHelper.CreateCollection(Article.CollectionId, Article.PartitionKey);
                await DbHelper.DeleteAllDocumentsAsync<Article>(Article.CollectionId);
                var titles = ConfigurationManager.AppSettings["WikipediaSampleArticles"].Split(',');
                await Article.Create(await Article.GetSampleArticles(titles));
                ViewBag.Message = "Articles deleted and recreated.";
            }
            if (button == "random")
            {
                await Article.Create(await Article.GetRadnomArticles());
                ViewBag.Message = "Random articles added.";
            }

            var diagnostics = new Diagnostics();
            return View(diagnostics);
        }

        [HttpGet]
        public async Task<ActionResult> Edit(Guid? articleId, string author, string message)
        {
            var article = !articleId.HasValue ? 
                Article.New() : 
                await Article.Read(articleId.Value, author);

            if (!string.IsNullOrEmpty(message))
                ViewBag.Message = message;

            return View(article);
        }

        [HttpPost]
        public async Task<ActionResult> Edit(Article article, string TagsStr, string button)
        {
            // Convert comma-separated list of tags to array
            article.Tags = TagsStr.Split(new[] { ',' }, StringSplitOptions.RemoveEmptyEntries).Select(t => t.Trim()).ToArray();
            if (article.Tags.Length == 0)
                ModelState.AddModelError("Tags", "Tags must not be empty.");

            if (!ModelState.IsValid)
            {
                var errors = string.Join("<br/>", ModelState.Values.SelectMany(i => i.Errors).Select(e => e.ErrorMessage));
                ViewBag.Message = $"Error:<br/>{errors}";
                return View(article);
            }

            button = (button ?? "").ToLower();
            switch (button)
            {
                case "save":
                    if (article.IsNew)
                        article.ArticleId = Guid.NewGuid();
                    await article.Upsert();
                    return RedirectToAction("Edit", new { article.ArticleId, article.Author, message = "Article saved" });
                case "preview":
                    ViewBag.EnablePreview = true;
                    break;
                case "delete":
                    await article.Delete();
                    ViewBag.Message = "Article deleted";
                    break;
            }

            return View(article);
        }

        [HttpGet]
        public ActionResult PerformanceTest()
        {
            return View();
        }

        [HttpPost]
        public async Task<ActionResult> PerformanceTest(PerformanceTest test)
        {
            try
            {
                await test.RunTests();
                ViewBag.Message = "Tests completed. Run the tests multiple times to get stable results.";
            }
            catch (Exception ex)
            {
                ViewBag.Message = $"<p>Tests failed.</p><p>{ex.GetType().Name}</p><p>{ex.Message}</p><p>{ex.StackTrace}</p>";
            }
            return View(test);
        }
    }
}